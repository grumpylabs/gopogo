package protocol

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/grumpylabs/gopogo/internal/cache"
)

// PostgresHandler serves the Postgres wire protocol (v3) on top of the shared
// command layer, as pogocache does. A query is a cache command, not SQL:
//
//	SET mykey 'hello world'        simple query
//	GET $1                         extended query with a bound parameter
//
// Both the simple and the extended (Parse/Bind/Describe/Execute/Sync) query
// protocols are supported. Results are sent in text format. BEGIN, COMMIT and
// ROLLBACK are accepted and ignored, and a leading ::bytea or ::text switches
// the column type for later results.
//
// With a TLS config, a client's SSLRequest upgrades the connection to TLS
// (sslmode=require and verify-full work). Passwords use SCRAM-SHA-256.
type PostgresHandler struct {
	exec      *Executor
	tlsConfig *tls.Config
}

// SetTLSConfig enables TLS upgrades for clients that send an SSLRequest.
func (h *PostgresHandler) SetTLSConfig(cfg *tls.Config) {
	h.tlsConfig = cfg
}

func NewPostgresHandler(cache *cache.Cache, auth, persist string) *PostgresHandler {
	return &PostgresHandler{exec: NewExecutor(cache, auth, persist)}
}

const (
	pgProtocolV3   = 196608
	sslRequestCode = 80877103
	cancelCode     = 80877102
	gssRequestCode = 80877104

	textOID  = 25
	byteaOID = 17

	maxPGMessage = 1 << 30
)

// pgToken is one parsed query argument. param is the 1-based parameter number
// for a $n placeholder (0 for a literal); join appends it to the previous
// argument, as in key$1.
type pgToken struct {
	text  string
	param int
	join  bool
}

type pgStatement struct {
	tokens  []pgToken
	nparams int
}

// pgPortal is a bound statement. Its command runs at Describe or Execute,
// whichever comes first, and the result is kept for Execute.
type pgPortal struct {
	args []string
	ran  bool
	res  result
}

type pgConn struct {
	h        *PostgresHandler
	conn     net.Conn // replaced by the TLS connection after an upgrade
	r        *bufio.Reader
	w        *bufio.Writer
	s        *session
	oid      int32
	stmts    map[string]*pgStatement
	portals  map[string]*pgPortal
	failed   bool // an extended-protocol error: discard messages until Sync
	quitting bool
}

var errPGProtocol = errors.New("postgres protocol error")

func (h *PostgresHandler) Handle(conn net.Conn) {
	c := &pgConn{
		h:       h,
		conn:    conn,
		r:       bufio.NewReader(conn),
		w:       bufio.NewWriter(conn),
		s:       &session{proto: TypePostgres, addr: conn.RemoteAddr().String()},
		oid:     textOID,
		stmts:   map[string]*pgStatement{},
		portals: map[string]*pgPortal{},
	}
	defer func() { c.conn.Close() }()
	if err := c.startup(); err != nil {
		c.w.Flush()
		return
	}
	for !c.quitting {
		typ, body, err := c.readMessage()
		if err != nil {
			return
		}
		if typ == 'X' { // Terminate
			return
		}
		if err := c.handle(typ, body); err != nil {
			c.w.Flush()
			return
		}
		// Like a Postgres server, reply to an extended-protocol batch only at
		// Sync or Flush, so pipelined messages are answered in one write.
		if typ == 'Q' || typ == 'S' || typ == 'H' || c.quitting {
			c.w.Flush()
		}
	}
}

// startup handles SSL/GSS requests, the startup message and password auth.
func (c *pgConn) startup() error {
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(c.r, hdr[:]); err != nil {
			return err
		}
		length := binary.BigEndian.Uint32(hdr[:4])
		code := binary.BigEndian.Uint32(hdr[4:])
		if length < 8 || length > 65536 {
			return errPGProtocol
		}
		rest := make([]byte, length-8)
		if _, err := io.ReadFull(c.r, rest); err != nil {
			return err
		}
		switch code {
		case sslRequestCode:
			if c.h.tlsConfig == nil || isTLSConn(c.conn) {
				// No TLS configured, or already encrypted: decline and the
				// client continues as it is.
				c.w.WriteByte('N')
				c.w.Flush()
				continue
			}
			// Anything already buffered was sent before encryption and could
			// be injected plaintext (CVE-2021-23214), so refuse it.
			if c.r.Buffered() > 0 {
				return errPGProtocol
			}
			c.w.WriteByte('S')
			c.w.Flush()
			tlsConn := tls.Server(c.conn, c.h.tlsConfig)
			if err := tlsConn.Handshake(); err != nil {
				return err
			}
			c.conn = tlsConn
			c.r = bufio.NewReader(tlsConn)
			c.w = bufio.NewWriter(tlsConn)
			continue
		case gssRequestCode:
			// GSSAPI encryption is not supported.
			c.w.WriteByte('N')
			c.w.Flush()
			continue
		case cancelCode:
			return errPGProtocol
		case pgProtocolV3:
		default:
			c.writeError(fmt.Sprintf("unsupported protocol version %d", code))
			return errPGProtocol
		}
		break
	}

	if c.h.exec.auth != "" {
		err := c.scramAuth(c.h.exec.auth)
		if err == nil || err == errSCRAM {
			countAuth(TypePostgres, err == nil)
		}
		if err != nil {
			if err == errSCRAM {
				c.writeError("WRONGPASS invalid username-password pair or user is disabled.")
			}
			return errPGProtocol
		}
	}
	c.s.authed = true

	c.writeMsg('R', be32(0)) // AuthenticationOk
	for _, kv := range [][2]string{
		{"client_encoding", "UTF8"},
		{"server_encoding", "UTF8"},
		{"server_version", Version + " (gopogo)"},
		{"standard_conforming_strings", "on"},
		{"integer_datetimes", "on"},
		{"DateStyle", "ISO, MDY"},
	} {
		c.writeMsg('S', append(append([]byte(kv[0]), 0), append([]byte(kv[1]), 0)...))
	}
	c.writeReady()
	c.w.Flush()
	return nil
}

// isTLSConn reports whether conn (possibly wrapped by the protocol detector)
// is already a TLS connection.
func isTLSConn(conn net.Conn) bool {
	if dc, ok := conn.(*detectorConn); ok {
		conn = dc.Conn
	}
	_, ok := conn.(*tls.Conn)
	return ok
}

func (c *pgConn) readMessage() (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(c.r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	if n < 4 || n > maxPGMessage {
		return 0, nil, errPGProtocol
	}
	body, err := readFull(c.r, int(n-4))
	if err != nil {
		return 0, nil, err
	}
	return hdr[0], body, nil
}

func (c *pgConn) handle(typ byte, body []byte) error {
	if c.failed && typ != 'S' {
		return nil // discard until Sync after an extended-protocol error
	}
	m := &pgMsg{b: body}
	switch typ {
	case 'Q':
		c.simpleQuery(m.cstr())
	case 'P':
		c.parse(m)
	case 'B':
		c.bind(m)
	case 'D':
		c.describe(m)
	case 'E':
		c.execute(m)
	case 'C':
		kind, name := m.byte1(), m.cstr()
		if kind == 'S' {
			delete(c.stmts, name)
		} else {
			delete(c.portals, name)
		}
		c.writeMsg('3', nil) // CloseComplete
	case 'S':
		c.failed = false
		c.writeReady()
	case 'H': // Flush: written by the caller
	default:
		c.writeError(fmt.Sprintf("unsupported message type '%c'", typ))
		c.writeReady()
	}
	if m.err {
		return errPGProtocol
	}
	return nil
}

func (c *pgConn) simpleQuery(query string) {
	tokens, nparams, err := parsePGQuery(query)
	switch {
	case err != nil:
		c.writeError(err.Error())
	case nparams > 0:
		c.writeError("query cannot have parameters")
	case len(tokens) == 0:
		c.writeMsg('I', nil) // EmptyQueryResponse
	default:
		res := c.run(bindTokens(tokens, nil))
		c.writeResult(res, true)
	}
	c.writeReady()
}

func (c *pgConn) parse(m *pgMsg) {
	name, query := m.cstr(), m.cstr()
	n := m.count()
	for i := 0; i < n; i++ {
		m.int32() // parameter type OIDs are ignored; everything is text
	}
	if m.err {
		return
	}
	tokens, nparams, err := parsePGQuery(query)
	if err != nil {
		c.fail(err.Error())
		return
	}
	c.stmts[name] = &pgStatement{tokens: tokens, nparams: nparams}
	c.writeMsg('1', nil) // ParseComplete
}

func (c *pgConn) bind(m *pgMsg) {
	portal, stmtName := m.cstr(), m.cstr()
	nformats := m.count()
	formats := make([]int16, nformats)
	for i := range formats {
		formats[i] = m.int16()
	}
	nparams := m.count()
	params := make([]string, nparams)
	for i := range params {
		n := m.int32()
		if n > 0 {
			params[i] = string(m.bytes(int(n)))
		}
		// Text and binary parameters are both taken as raw bytes; NULL is "".
	}
	nresult := m.count()
	for i := 0; i < nresult; i++ {
		m.int16() // results are always text
	}
	if m.err {
		return
	}
	stmt := c.stmts[stmtName]
	if stmt == nil {
		c.fail(fmt.Sprintf("prepared statement \"%s\" does not exist", stmtName))
		return
	}
	if nparams != stmt.nparams {
		c.fail(fmt.Sprintf("bind message supplies %d parameters, but prepared statement requires %d",
			nparams, stmt.nparams))
		return
	}
	c.portals[portal] = &pgPortal{args: bindTokens(stmt.tokens, params)}
	c.writeMsg('2', nil) // BindComplete
}

func (c *pgConn) describe(m *pgMsg) {
	kind, name := m.byte1(), m.cstr()
	if m.err {
		return
	}
	switch kind {
	case 'S':
		stmt := c.stmts[name]
		if stmt == nil {
			c.fail(fmt.Sprintf("prepared statement \"%s\" does not exist", name))
			return
		}
		// Parameters are text. Result columns are unknown until the
		// command runs, so the statement reports NoData; each execution
		// sends its own RowDescription.
		desc := be16(int16(stmt.nparams))
		for i := 0; i < stmt.nparams; i++ {
			desc = append(desc, be32(c.oid)...)
		}
		c.writeMsg('t', desc)
		c.writeMsg('n', nil)
	case 'P':
		p := c.portals[name]
		if p == nil {
			c.fail(fmt.Sprintf("portal \"%s\" does not exist", name))
			return
		}
		c.runPortal(p)
		if p.res.err == "" && p.res.pg.cols != nil {
			c.writeRowDescription(p.res.pg.cols)
		} else {
			c.writeMsg('n', nil)
		}
	default:
		c.fail(fmt.Sprintf("invalid describe type '%c'", kind))
	}
}

func (c *pgConn) execute(m *pgMsg) {
	name := m.cstr()
	m.int32() // max rows: all rows are always returned
	if m.err {
		return
	}
	p := c.portals[name]
	if p == nil {
		c.fail(fmt.Sprintf("portal \"%s\" does not exist", name))
		return
	}
	// If Describe ran the portal, its RowDescription was already sent.
	described := p.ran
	c.runPortal(p)
	if p.res.err != "" {
		c.fail(strings.TrimPrefix(p.res.err, "ERR "))
		return
	}
	c.writeResult(p.res, !described)
	// A portal runs once; executing it again repeats the stored result.
}

func (c *pgConn) runPortal(p *pgPortal) {
	if !p.ran {
		p.res = c.run(p.args)
		p.ran = true
	}
}

// run handles the Postgres-only commands, then the shared command table.
func (c *pgConn) run(args []string) result {
	if len(args) > 0 {
		switch strings.ToUpper(args[0]) {
		case "BEGIN", "COMMIT", "ROLLBACK":
			return result{pg: pgTag(strings.ToUpper(args[0]))}
		case "MONITOR":
			return errResult("unavailable")
		}
		if strings.HasPrefix(args[0], "::") {
			switch strings.ToLower(args[0]) {
			case "::bytea", "::bytes":
				c.oid = byteaOID
			case "::text":
				c.oid = textOID
			default:
				return errResult(fmt.Sprintf("unknown type '%s'", args[0][2:]))
			}
			args = args[1:]
			if len(args) == 0 {
				tag := "TEXT"
				if c.oid == byteaOID {
					tag = "BYTEA"
				}
				return result{pg: pgTag(tag)}
			}
		}
	}
	r := c.h.exec.exec(c.s, args)
	if r.quit {
		c.quitting = true
	}
	return r
}

// writeResult writes rows and the CommandComplete tag (or the error).
func (c *pgConn) writeResult(r result, withDesc bool) {
	if r.err != "" {
		c.writeError(strings.TrimPrefix(r.err, "ERR "))
		return
	}
	if r.pg.cols != nil && withDesc {
		c.writeRowDescription(r.pg.cols)
	}
	for _, row := range r.pg.rows {
		data := be16(int16(len(row)))
		for _, col := range row {
			v := col
			if c.oid == byteaOID {
				v = `\x` + hex.EncodeToString([]byte(col))
			}
			data = append(data, be32(int32(len(v)))...)
			data = append(data, v...)
		}
		c.writeMsg('D', data)
	}
	tag := r.pg.tag
	if tag == "" {
		tag = "OK"
	}
	c.writeMsg('C', append([]byte(tag), 0))
}

func (c *pgConn) writeRowDescription(cols []string) {
	desc := be16(int16(len(cols)))
	for _, col := range cols {
		desc = append(desc, col...)
		desc = append(desc, 0)
		desc = append(desc, be32(0)...) // table OID
		desc = append(desc, be16(0)...) // column number
		desc = append(desc, be32(c.oid)...)
		desc = append(desc, be16(-1)...) // type size
		desc = append(desc, be32(-1)...) // type modifier
		desc = append(desc, be16(0)...)  // text format
	}
	c.writeMsg('T', desc)
}

// fail reports an extended-protocol error and discards input until Sync.
func (c *pgConn) fail(msg string) {
	c.writeError(msg)
	c.failed = true
}

func (c *pgConn) writeError(msg string) {
	if !utf8.ValidString(msg) {
		msg = strings.ToValidUTF8(msg, "?")
	}
	var b []byte
	for _, f := range [][2]string{{"S", "ERROR"}, {"V", "ERROR"}, {"C", "XX000"}, {"M", msg}} {
		b = append(b, f[0][0])
		b = append(b, f[1]...)
		b = append(b, 0)
	}
	c.writeMsg('E', append(b, 0))
}

func (c *pgConn) writeReady() { c.writeMsg('Z', []byte{'I'}) }

func (c *pgConn) writeMsg(typ byte, body []byte) {
	c.w.WriteByte(typ)
	c.w.Write(be32(int32(len(body) + 4)))
	c.w.Write(body)
}

func be32(n int32) []byte { return binary.BigEndian.AppendUint32(nil, uint32(n)) }
func be16(n int16) []byte { return binary.BigEndian.AppendUint16(nil, uint16(n)) }

func cstring(b []byte) string {
	if i := strings.IndexByte(string(b), 0); i >= 0 {
		return string(b[:i])
	}
	return string(b)
}

// pgMsg reads fields from a message body. A short read sets err.
type pgMsg struct {
	b   []byte
	err bool
}

func (m *pgMsg) bytes(n int) []byte {
	if n < 0 || len(m.b) < n {
		m.err = true
		m.b = nil
		return nil
	}
	v := m.b[:n]
	m.b = m.b[n:]
	return v
}

func (m *pgMsg) byte1() byte {
	if b := m.bytes(1); b != nil {
		return b[0]
	}
	return 0
}

func (m *pgMsg) int16() int16 {
	if b := m.bytes(2); b != nil {
		return int16(binary.BigEndian.Uint16(b))
	}
	return 0
}

// count reads an Int16 element count. A negative count is malformed.
func (m *pgMsg) count() int {
	n := int(m.int16())
	if n < 0 {
		m.err = true
		m.b = nil
		return 0
	}
	return n
}

func (m *pgMsg) int32() int32 {
	if b := m.bytes(4); b != nil {
		return int32(binary.BigEndian.Uint32(b))
	}
	return 0
}

func (m *pgMsg) cstr() string {
	i := strings.IndexByte(string(m.b), 0)
	if i < 0 {
		m.err = true
		m.b = nil
		return ""
	}
	s := string(m.b[:i])
	m.b = m.b[i+1:]
	return s
}

// bindTokens substitutes parameters and joins adjacent tokens into command
// arguments.
func bindTokens(tokens []pgToken, params []string) []string {
	var args []string
	for _, t := range tokens {
		v := t.text
		if t.param > 0 {
			v = params[t.param-1]
		}
		if t.join && len(args) > 0 {
			args[len(args)-1] += v
		} else {
			args = append(args, v)
		}
	}
	return args
}

// parsePGQuery splits a query into arguments following pogocache's rules:
// whitespace-separated keywords, 'quoted strings' (” escapes a quote),
// E'escaped strings' (backslash escapes) and $n parameters. A token written
// directly after another, as in key$1, joins it. Parsing stops at ';', and
// double-quoted identifiers and dollar-quoted strings are rejected. It returns
// the tokens and the number of parameters, which must be $1..$n.
func parsePGQuery(q string) ([]pgToken, int, error) {
	var tokens []pgToken
	params := map[int]bool{}
	adjacent := false // the previous token ended right here, with no space
	add := func(t pgToken) {
		t.join = adjacent && len(tokens) > 0
		tokens = append(tokens, t)
		adjacent = true
	}
	i := 0
	for i < len(q) {
		ch := q[i]
		switch {
		case ch == ';':
			for ; i < len(q); i++ {
				if q[i] != ';' && !isPGSpace(q[i]) {
					return nil, 0, errors.New("unexpected characters at end of query")
				}
			}
		case isPGSpace(ch):
			adjacent = false
			i++
		case ch == '"':
			return nil, 0, errors.New("identifiers not allowed")
		case ch == '\'':
			s, n, err := pgQuoted(q[i+1:])
			if err != nil {
				return nil, 0, err
			}
			add(pgToken{text: s})
			i += 1 + n
		case (ch == 'E' || ch == 'e') && i+1 < len(q) && q[i+1] == '\'':
			s, n, err := pgEscaped(q[i+2:])
			if err != nil {
				return nil, 0, err
			}
			add(pgToken{text: s})
			i += 2 + n
		case ch == '$':
			j := i + 1
			for j < len(q) && q[j] >= '0' && q[j] <= '9' {
				j++
			}
			if j == i+1 {
				return nil, 0, errors.New("dollar-quote strings not allowed")
			}
			n, err := strconv.Atoi(q[i+1 : j])
			if err != nil || n == 0 || n > 0xFFFF {
				return nil, 0, fmt.Errorf("there is no parameter %s", q[i:j])
			}
			params[n] = true
			add(pgToken{param: n})
			i = j
		default:
			j := i
			for j < len(q) && !isPGSpace(q[j]) && q[j] != ';' && q[j] != '\'' && q[j] != '"' && q[j] != '$' {
				j++
			}
			add(pgToken{text: q[i:j]})
			i = j
		}
	}
	for n := 1; n <= len(params); n++ {
		if !params[n] {
			return nil, 0, fmt.Errorf("missing parameter $%d", n)
		}
	}
	return tokens, len(params), nil
}

func isPGSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

// pgQuoted reads a '...' string body (after the opening quote) and returns it
// with ” unescaped and the bytes consumed including the closing quote.
func pgQuoted(s string) (string, int, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			if i+1 < len(s) && s[i+1] == '\'' {
				b.WriteByte('\'')
				i++
				continue
			}
			return b.String(), i + 1, nil
		}
		b.WriteByte(s[i])
	}
	return "", 0, errors.New("unterminated quoted string")
}

// pgEscaped reads an E'...' string body with backslash escapes.
func pgEscaped(s string) (string, int, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			if i+1 < len(s) && s[i+1] == '\'' {
				b.WriteByte('\'')
				i++
				continue
			}
			return b.String(), i + 1, nil
		case '\\':
			i++
			if i == len(s) {
				return "", 0, errors.New("unterminated quoted string")
			}
			switch s[i] {
			case 'b':
				b.WriteByte('\b')
			case 'f':
				b.WriteByte('\f')
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case 'u':
				if i+4 < len(s) {
					if cp, err := strconv.ParseUint(s[i+1:i+5], 16, 32); err == nil {
						b.WriteRune(rune(cp))
						i += 4
						continue
					}
				}
				b.WriteByte('u')
			default:
				b.WriteByte(s[i])
			}
		default:
			b.WriteByte(s[i])
		}
	}
	return "", 0, errors.New("unterminated quoted string")
}

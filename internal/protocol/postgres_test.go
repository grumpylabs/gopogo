package protocol

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/binary"
	"io"
	"math/big"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
)

func TestParsePGQuery(t *testing.T) {
	tests := []struct {
		query   string
		params  []string
		want    []string
		wantErr bool
	}{
		{query: "SET k v", want: []string{"SET", "k", "v"}},
		{query: "  GET   k ;", want: []string{"GET", "k"}},
		{query: "SET k 'hello world'", want: []string{"SET", "k", "hello world"}},
		{query: "SET k 'it''s'", want: []string{"SET", "k", "it's"}},
		{query: `SET k E'a\tb\n\'q\''`, want: []string{"SET", "k", "a\tb\n'q'"}},
		{query: `SET k E'é'`, want: []string{"SET", "k", "é"}},
		{query: "SET $1 $2", params: []string{"key", "val"}, want: []string{"SET", "key", "val"}},
		{query: "GET user:$1", params: []string{"42"}, want: []string{"GET", "user:42"}},
		{query: "GET 'a'$1'b'", params: []string{"-"}, want: []string{"GET", "a-b"}},
		{query: "", want: nil},
		{query: "GET k; DEL k", wantErr: true},
		{query: `GET "k"`, wantErr: true},
		{query: "GET $$x$$", wantErr: true},
		{query: "GET 'open", wantErr: true},
		{query: "GET $2", wantErr: true},
		{query: "GET $0", wantErr: true},
	}
	for _, tt := range tests {
		tokens, nparams, err := parsePGQuery(tt.query)
		if tt.wantErr {
			if err == nil {
				t.Errorf("%q: expected error", tt.query)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", tt.query, err)
			continue
		}
		if nparams != len(tt.params) {
			t.Errorf("%q: nparams %d, want %d", tt.query, nparams, len(tt.params))
			continue
		}
		if got := bindTokens(tokens, tt.params); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q: got %q, want %q", tt.query, got, tt.want)
		}
	}
}

// pgTestClient speaks just enough of the Postgres protocol for tests.
type pgTestClient struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

type pgTestMsg struct {
	typ  byte
	body []byte
}

func newPGTestClient(t *testing.T, c *cache.Cache) *pgTestClient {
	t.Helper()
	server, client := net.Pipe()
	go NewPostgresHandler(c, "", "").Handle(server)
	t.Cleanup(func() { client.Close() })
	p := &pgTestClient{t: t, conn: client, r: bufio.NewReader(client)}

	// SSLRequest, then the startup message.
	p.write(nil, be32(sslRequestCode))
	if b, _ := p.r.ReadByte(); b != 'N' {
		t.Fatalf("SSLRequest reply %q, want 'N'", b)
	}
	startup := append(be32(pgProtocolV3), "user\x00test\x00\x00"...)
	p.write(nil, startup)
	p.readUntilReady()
	return p
}

// write sends a message; typ nil means an untyped startup-phase message.
func (p *pgTestClient) write(typ *byte, body []byte) {
	p.t.Helper()
	var msg []byte
	if typ != nil {
		msg = append(msg, *typ)
	}
	msg = append(msg, be32(int32(len(body)+4))...)
	msg = append(msg, body...)
	p.conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := p.conn.Write(msg); err != nil {
		p.t.Fatal(err)
	}
}

func (p *pgTestClient) send(typ byte, body []byte) { p.write(&typ, body) }

func (p *pgTestClient) readUntilReady() []pgTestMsg {
	p.t.Helper()
	var msgs []pgTestMsg
	for {
		var hdr [5]byte
		if _, err := io.ReadFull(p.r, hdr[:]); err != nil {
			p.t.Fatal(err)
		}
		body := make([]byte, binary.BigEndian.Uint32(hdr[1:])-4)
		if _, err := io.ReadFull(p.r, body); err != nil {
			p.t.Fatal(err)
		}
		msgs = append(msgs, pgTestMsg{hdr[0], body})
		if hdr[0] == 'Z' {
			return msgs
		}
	}
}

// summarize renders messages as "type:detail" for comparison: DataRow values,
// CommandComplete tags and ErrorResponse messages.
func summarize(msgs []pgTestMsg) []string {
	var out []string
	for _, m := range msgs {
		switch m.typ {
		case 'D':
			b := m.body[2:]
			s := "D:"
			for len(b) >= 4 {
				n := int(int32(binary.BigEndian.Uint32(b)))
				s += string(b[4:4+n]) + "|"
				b = b[4+n:]
			}
			out = append(out, s)
		case 'C':
			out = append(out, "C:"+cstring(m.body))
		case 'E':
			for f := m.body; len(f) > 1; {
				code, rest := f[0], f[1:]
				v := cstring(rest)
				if code == 'M' {
					out = append(out, "E:"+v)
				}
				f = rest[len(v)+1:]
			}
		default:
			out = append(out, string(m.typ))
		}
	}
	return out
}

func cstr(s string) []byte { return append([]byte(s), 0) }

func TestPostgresSimpleQuery(t *testing.T) {
	p := newPGTestClient(t, cache.New(nil))

	query := func(q string) []string {
		p.send('Q', cstr(q))
		return summarize(p.readUntilReady())
	}
	expect := func(q string, want ...string) {
		t.Helper()
		if got := query(q); !reflect.DeepEqual(got, want) {
			t.Fatalf("%q: got %q, want %q", q, got, want)
		}
	}
	expect("SET k 'v 1'", "C:SET 1", "Z")
	expect("GET k", "T", "D:v 1|", "C:GET 1", "Z")
	expect("GET missing", "T", "C:GET 0", "Z")
	expect("DBSIZE", "T", "D:1|", "C:DBSIZE", "Z")
	expect("BEGIN", "C:BEGIN", "Z")
	expect("", "I", "Z")
	expect("SET $1 v", "E:query cannot have parameters", "Z")
	expect("NOPE", "E:unknown command 'NOPE'", "Z")
	expect("SELECT 0", "E:unknown command 'SELECT'", "Z")
}

func TestPostgresExtendedQuery(t *testing.T) {
	p := newPGTestClient(t, cache.New(nil))

	// Parse + Describe statement + Sync, as pgx's Prepare does.
	parse := append(cstr("s1"), cstr("SET $1 $2")...)
	parse = append(parse, be16(0)...)
	p.send('P', parse)
	p.send('D', append([]byte{'S'}, cstr("s1")...))
	p.send('S', nil)
	if got := summarize(p.readUntilReady()); !reflect.DeepEqual(got, []string{"1", "t", "n", "Z"}) {
		t.Fatalf("prepare: got %q", got)
	}

	bind := func(stmt string, params ...string) []byte {
		b := append(cstr(""), cstr(stmt)...)
		b = append(b, be16(0)...)
		b = append(b, be16(int16(len(params)))...)
		for _, v := range params {
			b = append(b, be32(int32(len(v)))...)
			b = append(b, v...)
		}
		return append(b, be16(0)...)
	}
	// Bind + Describe portal + Execute + Sync, as pgx's ExecPrepared does.
	run := func(stmt string, params ...string) []string {
		p.send('B', bind(stmt, params...))
		p.send('D', append([]byte{'P'}, cstr("")...))
		p.send('E', append(cstr(""), be32(0)...))
		p.send('S', nil)
		return summarize(p.readUntilReady())
	}
	if got := run("s1", "key", "value"); !reflect.DeepEqual(got, []string{"2", "n", "C:SET 1", "Z"}) {
		t.Fatalf("SET: got %q", got)
	}

	p.send('P', append(append(cstr("s2"), cstr("GET $1")...), be16(0)...))
	p.send('S', nil)
	p.readUntilReady()
	if got := run("s2", "key"); !reflect.DeepEqual(got, []string{"2", "T", "D:value|", "C:GET 1", "Z"}) {
		t.Fatalf("GET: got %q", got)
	}

	// After an error, messages are discarded until Sync.
	p.send('B', bind("nope"))
	p.send('E', append(cstr(""), be32(0)...))
	p.send('S', nil)
	got := summarize(p.readUntilReady())
	want := []string{`E:prepared statement "nope" does not exist`, "Z"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("error recovery: got %q, want %q", got, want)
	}
}

// pgReadMsg reads one backend message.
func pgReadMsg(t *testing.T, r *bufio.Reader) (byte, []byte) {
	t.Helper()
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		t.Fatal(err)
	}
	body := make([]byte, binary.BigEndian.Uint32(hdr[1:])-4)
	if _, err := io.ReadFull(r, body); err != nil {
		t.Fatal(err)
	}
	return hdr[0], body
}

func pgWriteMsg(t *testing.T, w io.Writer, typ byte, body []byte) {
	t.Helper()
	msg := append([]byte{typ}, be32(int32(len(body)+4))...)
	if _, err := w.Write(append(msg, body...)); err != nil {
		t.Fatal(err)
	}
}

// scramLogin runs a client-side SCRAM-SHA-256 exchange and returns the
// message that follows it (AuthenticationOk or an error).
func scramLogin(t *testing.T, conn io.ReadWriter, r *bufio.Reader, password string) (byte, []byte) {
	t.Helper()
	startup := append(be32(pgProtocolV3), "user\x00test\x00\x00"...)
	conn.Write(append(be32(int32(len(startup)+4)), startup...))

	typ, body := pgReadMsg(t, r)
	if typ != 'R' || binary.BigEndian.Uint32(body) != 10 || !strings.Contains(string(body[4:]), "SCRAM-SHA-256") {
		t.Fatalf("expected AuthenticationSASL, got %c %q", typ, body)
	}
	clientFirstBare := "n=,r=clientnonce123"
	first := "n,," + clientFirstBare
	init := append(cstr("SCRAM-SHA-256"), be32(int32(len(first)))...)
	pgWriteMsg(t, conn, 'p', append(init, first...))

	typ, body = pgReadMsg(t, r)
	if typ != 'R' || binary.BigEndian.Uint32(body) != 11 {
		t.Fatalf("expected AuthenticationSASLContinue, got %c %q", typ, body)
	}
	serverFirst := string(body[4:])
	salt, _ := base64.StdEncoding.DecodeString(scramAttr(serverFirst, 's'))
	iter, _ := strconv.Atoi(scramAttr(serverFirst, 'i'))
	withoutProof := "c=biws,r=" + scramAttr(serverFirst, 'r')
	salted, _ := pbkdf2.Key(sha256.New, password, salt, iter, sha256.Size)
	clientKey := scramHMAC(salted, "Client Key")
	storedKey := sha256.Sum256(clientKey)
	sig := scramHMAC(storedKey[:], clientFirstBare+","+serverFirst+","+withoutProof)
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ sig[i]
	}
	pgWriteMsg(t, conn, 'p', []byte(withoutProof+",p="+base64.StdEncoding.EncodeToString(proof)))
	return pgReadMsg(t, r)
}

func TestPostgresSCRAM(t *testing.T) {
	for _, tt := range []struct {
		password string
		ok       bool
	}{{"s3cret", true}, {"wrong", false}} {
		server, client := net.Pipe()
		go NewPostgresHandler(cache.New(nil), "s3cret", "").Handle(server)
		client.SetDeadline(time.Now().Add(5 * time.Second))
		r := bufio.NewReader(client)
		typ, body := scramLogin(t, client, r, tt.password)
		if tt.ok {
			if typ != 'R' || binary.BigEndian.Uint32(body) != 12 {
				t.Fatalf("expected AuthenticationSASLFinal, got %c %q", typ, body)
			}
			if typ, body = pgReadMsg(t, r); typ != 'R' || binary.BigEndian.Uint32(body) != 0 {
				t.Fatalf("expected AuthenticationOk, got %c %q", typ, body)
			}
		} else if typ != 'E' || !strings.Contains(string(body), "WRONGPASS") {
			t.Fatalf("wrong password: got %c %q", typ, body)
		}
		client.Close()
	}
}

func testTLSConfig(t *testing.T) (*tls.Config, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "gopogo-test"},
		DNSNames:              []string{"gopogo-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}, pool
}

func TestPostgresTLSUpgrade(t *testing.T) {
	serverCfg, pool := testTLSConfig(t)
	h := NewPostgresHandler(cache.New(nil), "", "")
	h.SetTLSConfig(serverCfg)

	server, client := net.Pipe()
	go h.Handle(server)
	defer client.Close()
	client.SetDeadline(time.Now().Add(5 * time.Second))

	client.Write(append(be32(8), be32(sslRequestCode)...))
	var reply [1]byte
	if _, err := io.ReadFull(client, reply[:]); err != nil || reply[0] != 'S' {
		t.Fatalf("SSLRequest reply %q, %v; want 'S'", reply[0], err)
	}
	tc := tls.Client(client, &tls.Config{RootCAs: pool, ServerName: "gopogo-test"})
	if err := tc.Handshake(); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(tc)
	startup := append(be32(pgProtocolV3), "user\x00test\x00\x00"...)
	tc.Write(append(be32(int32(len(startup)+4)), startup...))
	for {
		typ, _ := pgReadMsg(t, r)
		if typ == 'Z' {
			break
		}
	}
	query := func(q string) []string {
		pgWriteMsg(t, tc, 'Q', cstr(q))
		var out []string
		for {
			typ, body := pgReadMsg(t, r)
			switch typ {
			case 'D':
				out = append(out, string(body[6:]))
			case 'C':
				out = append(out, cstring(body))
			case 'Z':
				return out
			}
		}
	}
	if got := query("SET k over-tls"); !reflect.DeepEqual(got, []string{"SET 1"}) {
		t.Fatalf("SET over TLS: got %q", got)
	}
	if got := query("GET k"); !reflect.DeepEqual(got, []string{"over-tls", "GET 1"}) {
		t.Fatalf("GET over TLS: got %q", got)
	}
}

func TestPostgresTLSRejectsInjectedPlaintext(t *testing.T) {
	serverCfg, _ := testTLSConfig(t)
	h := NewPostgresHandler(cache.New(nil), "", "")
	h.SetTLSConfig(serverCfg)
	server, client := net.Pipe()
	go h.Handle(server)
	defer client.Close()
	client.SetDeadline(time.Now().Add(5 * time.Second))

	// An SSLRequest with plaintext bytes after it in the same write must not
	// be upgraded: the extra bytes would bypass encryption.
	client.Write(append(append(be32(8), be32(sslRequestCode)...), "Qinjected"...))
	if b, err := bufio.NewReader(client).ReadByte(); err == nil {
		t.Fatalf("expected the connection to close, got %q", b)
	}
}

func TestPostgresNoTLSDeclines(t *testing.T) {
	newPGTestClient(t, cache.New(nil)) // asserts the 'N' reply
}

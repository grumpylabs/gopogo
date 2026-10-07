package protocol

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/grumpylabs/gopogo/internal/cache"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
)

// HTTPHandler maps HTTP requests onto the shared command layer, as pogocache
// does:
//
//	GET    /key                               -> GET key
//	PUT    /key?ex=&flags=&cas=&nx&xx  (body) -> SET key body ...
//	DELETE /key                               -> DEL key
//
// POST is accepted as PUT, and the X-TTL, X-Flags and X-CAS headers as the
// ex, flags and cas query parameters. HEAD reports a key's size, flags and
// CAS without a body. Paths starting with '@' are reserved: /@stats and
// /@keys?pattern= return JSON. Authentication uses ?auth= or an
// "Authorization: Bearer" header.
type HTTPHandler struct {
	exec *Executor
}

func NewHTTPHandler(cache *cache.Cache, auth string) *HTTPHandler {
	return &HTTPHandler{exec: NewExecutor(cache, auth, "")}
}

const httpHelp = "gopogo HTTP interface\r\n\r\n" +
	"GET /<key>\r\n" +
	"PUT /<key>?ex=<seconds>&flags=<n>&cas=<n>&nx&xx  (value in body)\r\n" +
	"DELETE /<key>\r\n"

func (h *HTTPHandler) Handle(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := &httpWriter{Writer: bufio.NewWriterSize(conn, replyBufferSize)}
	_, isTLS := conn.(*tls.Conn)

	for {
		req, err := http.ReadRequest(reader)
		if err != nil {
			if err != io.EOF {
				h.writeText(writer, http.StatusBadRequest, "Bad Request\r\n")
				writer.Flush()
			}
			return
		}
		h.serveTraced(writer, req, conn.RemoteAddr().String(), isTLS)
		if reader.Buffered() == 0 || req.Close {
			writer.Flush()
		}
		if req.Close {
			return
		}
	}
}

// httpWriter is a connection's response writer; it remembers the status of
// the last response written.
type httpWriter struct {
	*bufio.Writer
	status int
}

// serveTraced serves one request in an HTTP server span, continuing the
// caller's trace from traceparent, and records http.server.request.duration.
// The cache command runs in a child span. Following gopogo's rule that keys
// are never recorded, the span has http.route (/{key}) but no url.path.
func (h *HTTPHandler) serveTraced(w *httpWriter, req *http.Request, addr string, isTLS bool) {
	start := time.Now()
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(req.Header))
	method := httpMethod(req.Method)
	route := httpRoute(req.URL.EscapedPath())
	scheme := "http"
	if isTLS {
		scheme = "https"
	}
	attrs := []attribute.KeyValue{
		semconv.HTTPRequestMethodKey.String(method),
		semconv.URLScheme(scheme),
		semconv.NetworkProtocolName("http"),
		semconv.NetworkProtocolVersion(fmt.Sprintf("%d.%d", req.ProtoMajor, req.ProtoMinor)),
	}
	if route != "" {
		attrs = append(attrs, semconv.HTTPRoute(route))
	}

	var span trace.Span
	if tracingEnabled.Load() {
		spanAttrs := append([]attribute.KeyValue{}, attrs...)
		if method == "_OTHER" {
			spanAttrs = append(spanAttrs, semconv.HTTPRequestMethodOriginal(req.Method))
		}
		if host, port := splitClient(addr); host != "" {
			spanAttrs = append(spanAttrs, semconv.ClientAddress(host), semconv.ClientPort(port))
		}
		if ua := req.UserAgent(); ua != "" {
			spanAttrs = append(spanAttrs, semconv.UserAgentOriginal(ua))
		}
		name := method
		if method == "_OTHER" {
			name = "HTTP"
		}
		if route != "" {
			name += " " + route
		}
		ctx, span = otel.Tracer(instrumentationName).Start(ctx, name,
			trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(spanAttrs...))
	}

	w.status = 0
	h.serve(ctx, w, req, addr)

	result := []attribute.KeyValue{semconv.HTTPResponseStatusCode(w.status)}
	if w.status >= 500 {
		result = append(result, semconv.ErrorTypeKey.String(strconv.Itoa(w.status)))
	}
	if span != nil {
		span.SetAttributes(result...)
		if w.status >= 500 {
			span.SetStatus(codes.Error, http.StatusText(w.status))
		}
		span.End()
	}
	if hist := httpDuration.Load(); hist != nil {
		(*hist).Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(append(attrs, result...)...))
	}
}

// httpMethod returns a known method as is and any other as _OTHER, so
// clients cannot create unbounded attribute values.
func httpMethod(m string) string {
	switch m {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodDelete,
		http.MethodConnect, http.MethodOptions, http.MethodTrace, http.MethodPatch:
		return m
	}
	return "_OTHER"
}

// httpRoute is the route a path matched, without the key, or "" for a
// reserved path gopogo does not serve.
func httpRoute(path string) string {
	key := strings.TrimPrefix(path, "/")
	switch {
	case key == "":
		return "/"
	case key == "@stats" || key == "@keys":
		return "/" + key
	case strings.HasPrefix(key, "@"):
		return ""
	}
	return "/{key}"
}

func (h *HTTPHandler) serve(ctx context.Context, w *httpWriter, req *http.Request, addr string) {
	query, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		h.writeText(w, http.StatusBadRequest, "Bad Request\r\n")
		return
	}
	if !h.authorized(req, query) {
		h.writeText(w, http.StatusUnauthorized, "Unauthorized\r\n")
		return
	}

	key := strings.TrimPrefix(req.URL.EscapedPath(), "/")
	switch {
	case key == "" && (req.Method == http.MethodGet || req.Method == http.MethodHead):
		h.writeText(w, http.StatusOK, httpHelp)
		return
	case key == "@stats" && req.Method == http.MethodGet:
		h.writeJSON(w, h.exec.cache.Stats())
		return
	case key == "@keys" && req.Method == http.MethodGet:
		h.writeKeys(w, query.Get("pattern"))
		return
	case strings.HasPrefix(key, "@"):
		h.writeText(w, http.StatusBadRequest, "Bad Request\r\n")
		return
	}
	if !validHTTPKey(key) {
		h.writeText(w, http.StatusBadRequest, "Invalid Key\r\n")
		return
	}

	var args []string
	switch req.Method {
	case http.MethodGet:
		args = []string{"GET", key}
	case http.MethodHead:
		h.serveHead(w, key)
		return
	case http.MethodPut, http.MethodPost:
		body, err := io.ReadAll(req.Body)
		if err != nil {
			h.writeText(w, http.StatusBadRequest, "Bad Request\r\n")
			return
		}
		args = []string{"SET", key, string(body)}
		param := func(name, header string) string {
			if v := query.Get(name); v != "" {
				return v
			}
			return req.Header.Get(header)
		}
		ex := param("ex", "X-TTL")
		if ex == "" {
			ex = query.Get("ttl")
		}
		if ex != "" {
			args = append(args, "EX", ex)
		}
		if v := param("flags", "X-Flags"); v != "" {
			args = append(args, "FLAGS", v)
		}
		if v := param("cas", "X-CAS"); v != "" {
			args = append(args, "CAS", v)
		}
		if query.Has("nx") {
			args = append(args, "NX")
		}
		if query.Has("xx") {
			args = append(args, "XX")
		}
	case http.MethodDelete:
		args = []string{"DEL", key}
	default:
		h.writeText(w, http.StatusMethodNotAllowed, "Method Not Allowed\r\n")
		return
	}

	s := &session{proto: TypeHTTP, addr: addr, authed: true, ctx: ctx}
	r := h.exec.exec(s, args)
	switch {
	case r.err != "":
		h.writeText(w, http.StatusInternalServerError, "ERR "+strings.TrimPrefix(r.err, "ERR ")+"\r\n")
	case r.http != nil:
		h.writeText(w, r.http.status, r.http.body)
	default:
		h.writeText(w, http.StatusBadRequest, "Bad Request\r\n")
	}
}

// authorized checks ?auth= or a Bearer token. As in pogocache, a credential
// is checked when one is supplied even if the server has no password.
func (h *HTTPHandler) authorized(req *http.Request, query url.Values) bool {
	token, given := query.Get("auth"), query.Has("auth")
	if !given {
		if hdr := req.Header.Get("Authorization"); hdr != "" {
			if !strings.HasPrefix(hdr, "Bearer ") {
				return false
			}
			token, given = hdr[len("Bearer "):], true
		}
	}
	if h.exec.auth == "" && !given {
		return true
	}
	ok := token == h.exec.auth
	countAuth(TypeHTTP, ok)
	return ok
}

// validHTTPKey matches pogocache's rule: 1-250 printable ASCII bytes, none of
// which is '%', '+', '@', '$', '?' or '='.
func validHTTPKey(key string) bool {
	if len(key) == 0 || len(key) > 250 {
		return false
	}
	for i := 0; i < len(key); i++ {
		switch c := key[i]; {
		case c <= ' ' || c >= 0x7F:
			return false
		case c == '%' || c == '+' || c == '@' || c == '$' || c == '?' || c == '=':
			return false
		}
	}
	return true
}

func (h *HTTPHandler) serveHead(w *httpWriter, key string) {
	entry, found := h.exec.cache.Load([]byte(key))
	if !found {
		h.writeResponse(w, http.StatusNotFound, map[string]string{"Content-Length": "0"}, nil)
		return
	}
	h.writeResponse(w, http.StatusOK, map[string]string{
		"Content-Type":   "application/octet-stream",
		"Content-Length": strconv.Itoa(len(entry.Value())),
		"X-Flags":        strconv.FormatUint(uint64(entry.Flags()), 10),
		"X-CAS":          strconv.FormatUint(entry.CAS(), 10),
	}, nil)
}

func (h *HTTPHandler) writeKeys(w *httpWriter, pattern string) {
	if pattern == "" {
		pattern = "*"
	}
	keys := make([]string, 0)
	h.exec.cache.Iterate(func(entry *cache.Entry) bool {
		key := string(entry.Key())
		if matchPattern(pattern, key) {
			keys = append(keys, key)
		}
		return true
	})
	h.writeJSON(w, keys)
}

func (h *HTTPHandler) writeJSON(w *httpWriter, v interface{}) {
	body, _ := json.MarshalIndent(v, "", "  ")
	h.writeResponse(w, http.StatusOK, map[string]string{"Content-Type": "application/json"}, body)
}

func (h *HTTPHandler) writeText(w *httpWriter, status int, body string) {
	h.writeResponse(w, status, map[string]string{"Content-Type": "text/plain"}, []byte(body))
}

// writeResponse writes a response with a Content-Length for body. HEAD
// responses pass a nil body and set Content-Length in headers instead.
func (h *HTTPHandler) writeResponse(w *httpWriter, status int, headers map[string]string, body []byte) {
	w.status = status
	w.WriteString(fmt.Sprintf("HTTP/1.1 %d %s\r\n", status, http.StatusText(status)))
	w.WriteString("Server: gopogo/" + Version + "\r\n")
	w.WriteString("Date: " + time.Now().UTC().Format(http.TimeFormat) + "\r\n")
	for key, value := range headers {
		w.WriteString(key + ": " + value + "\r\n")
	}
	if _, ok := headers["Content-Length"]; !ok {
		w.WriteString("Content-Length: " + strconv.Itoa(len(body)) + "\r\n")
	}
	w.WriteString("\r\n")
	w.Write(body)
}

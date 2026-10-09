package protocol

type Type int

const (
	TypeUnknown Type = iota
	TypeRedis
	TypeHTTP
	TypeMemcache
	TypePostgres
)

// String returns the protocol name used in metrics and spans.
func (t Type) String() string {
	switch t {
	case TypeRedis:
		return "redis"
	case TypeHTTP:
		return "http"
	case TypeMemcache:
		return "memcache"
	case TypePostgres:
		return "postgres"
	}
	return "unknown"
}

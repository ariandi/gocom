package constant

type (
	HeaderKey string
)

const (
	RequestIDHeader     HeaderKey = "X-Request-ID"
	CorrelationIDHeader HeaderKey = "X-Correlation-ID"
	TracerIDHeader      HeaderKey = "X-Trace-ID"
)

func (hk HeaderKey) String() string {
	return string(hk)
}

package observability

type tracerID string
type spanID string

type tracerNames struct {
	Proxy tracerID
}

type proxySpanNames struct {
	Request spanID
}

type spanNames struct {
	Proxy proxySpanNames
}

var tracerName = tracerNames{
	Proxy: "versiond.proxy",
}

var spanName = spanNames{
	Proxy: proxySpanNames{
		Request: "versiond.proxy.request",
	},
}
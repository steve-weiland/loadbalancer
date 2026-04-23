package proxy

import (
	"net"
	"net/http"
	"strings"
)

// hopByHopHeaders per RFC 7230 §6.1. Stripped from both directions.
var hopByHopHeaders = map[string]struct{}{
	"Connection":          {},
	"Keep-Alive":          {},
	"Proxy-Authenticate":  {},
	"Proxy-Authorization": {},
	"TE":                  {},
	"Trailer":             {},
	"Transfer-Encoding":   {},
	"Upgrade":             {},
}

func isHopByHop(name string) bool {
	_, ok := hopByHopHeaders[http.CanonicalHeaderKey(name)]
	return ok
}

// copyEndToEndHeaders copies every header from src to dst except hop-by-hop
// headers and any header named in src's Connection field.
func copyEndToEndHeaders(dst, src http.Header) {
	conn := src.Values("Connection")
	connNames := map[string]struct{}{}
	for _, c := range conn {
		for _, name := range strings.Split(c, ",") {
			if name = strings.TrimSpace(name); name != "" {
				connNames[http.CanonicalHeaderKey(name)] = struct{}{}
			}
		}
	}
	for k, vv := range src {
		ck := http.CanonicalHeaderKey(k)
		if _, isHop := hopByHopHeaders[ck]; isHop {
			continue
		}
		if _, named := connNames[ck]; named {
			continue
		}
		for _, v := range vv {
			dst.Add(ck, v)
		}
	}
}

// stripHopByHopFromConnection removes any header on dst named in src's
// Connection field. Defensive against a Connection field appearing on the
// outbound request after copyEndToEndHeaders runs.
func stripHopByHopFromConnection(src, dst http.Header) {
	for _, c := range src.Values("Connection") {
		for _, name := range strings.Split(c, ",") {
			if name = strings.TrimSpace(name); name != "" {
				dst.Del(name)
			}
		}
	}
}

// setForwardedHeaders sets X-Forwarded-For (appending) and X-Forwarded-Proto.
func setForwardedHeaders(out *http.Request, in *http.Request) {
	if clientIP, _, err := net.SplitHostPort(in.RemoteAddr); err == nil {
		if prior := in.Header.Get("X-Forwarded-For"); prior != "" {
			out.Header.Set("X-Forwarded-For", prior+", "+clientIP)
		} else {
			out.Header.Set("X-Forwarded-For", clientIP)
		}
	}
	if in.TLS != nil {
		out.Header.Set("X-Forwarded-Proto", "https")
	} else {
		out.Header.Set("X-Forwarded-Proto", "http")
	}
}

// Command echobackend is a tiny HTTP server used as the upstream in
// run-cluster and docker-compose. It returns a JSON document echoing the
// request and the backend's id so you can see round-robin in action.
//
//	echobackend --listen=:9001 --id=b1
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
)

type echo struct {
	Backend string `json:"backend"`
	Method  string `json:"method"`
	Path    string `json:"path"`
}

func main() {
	listen := flag.String("listen", ":9001", "HTTP listen address")
	id := flag.String("id", "echo", "backend id echoed in the response")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(echo{
			Backend: *id,
			Method:  r.Method,
			Path:    r.URL.Path,
		})
	})

	log.Printf("echobackend %s listening on %s", *id, *listen)
	if err := http.ListenAndServe(*listen, mux); err != nil {
		log.Fatal(err)
	}
}

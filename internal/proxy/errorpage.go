package proxy

import (
	"encoding/json"
	"net/http"
)

type errorBody struct {
	Error    string `json:"error"`
	Backend  string `json:"backend,omitempty"`
	Attempts int    `json:"attempts"`
}

func (p *Proxy) writeError(w http.ResponseWriter, status int, msg, backend string, attempts int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{Error: msg, Backend: backend, Attempts: attempts})
}

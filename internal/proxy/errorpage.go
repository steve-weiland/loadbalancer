package proxy

import (
	"encoding/json"
	"net/http"
)

type errorBody struct {
	Error   string `json:"error"`
	Backend string `json:"backend,omitempty"`
}

func writeError(w http.ResponseWriter, status int, msg, backend string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{Error: msg, Backend: backend})
}

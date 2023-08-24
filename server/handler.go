package server

import (
	"encoding/json"
	"inmemorydb/core"
	"net/http"
)

type Handler struct {
	db *core.InMemoryDb
}

func NewHandler(db *core.InMemoryDb) *Handler {
	handler := &Handler{
		db: db,
	}

	return handler
}

type Request struct {
	Command *string `json:"command,omitempty"`
}

func (h *Handler) Command(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		w.Header().Set("Content-type", "application/json")
		req := &Request{}
		// Decode reuqest body
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Validate request body
		resp := map[string]interface{}{}
		if req.Command == nil {
			resp["error"] = "invalid command"
			respByte, err := json.Marshal(resp)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
			w.Write(respByte)
			return
		}

		// Execute database command
		dbResponse, err := h.db.Command(*req.Command)
		if err != nil {
			resp["error"] = err.Error()
			respByte, err := json.Marshal(resp)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
			w.Write(respByte)
			return
		}

		if dbResponse == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		} else {
			w.WriteHeader(http.StatusOK)
		}

		// Return response from database
		resp["value"] = dbResponse
		respByte, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Write(respByte)
	} else {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
}

func (h *Handler) Status(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		resp := map[string]interface{}{
			"status": "ok",
		}
		respByte, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(respByte)
	} else {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
}
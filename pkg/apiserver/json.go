package apiserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/store"
)

// APIError is the single error shape every failure returns.
//
// Clients get a stable machine-readable code plus a message meant for a human,
// and validation failures carry the exact fields at fault, so the console can
// mark the offending input instead of showing a wall of text.
type APIError struct {
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Fields  []v1.FieldError `json:"fields,omitempty"`
}

type errorEnvelope struct {
	Error APIError `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// Orion's API is not browser-embeddable and returns no HTML, but the
	// console fetches it, so these two headers cost nothing and remove a class
	// of mistakes.
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)

	enc := json.NewEncoder(w)
	if err := enc.Encode(body); err != nil {
		// The status line is already sent; all that is left is to record it.
		return
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorEnvelope{Error: APIError{Code: code, Message: message}})
}

func writeValidationError(w http.ResponseWriter, err error) {
	ve, ok := v1.AsValidationError(err)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	writeJSON(w, http.StatusUnprocessableEntity, errorEnvelope{Error: APIError{
		Code:    "validation_failed",
		Message: fmt.Sprintf("%d field(s) failed validation", len(ve.Errors)),
		Fields:  ve.Errors,
	}})
}

// writeResult returns whichever object a write produced.
func writeResult(w http.ResponseWriter, status int, res store.Result) {
	switch {
	case res.Workload != nil:
		writeJSON(w, status, res.Workload)
	case res.Deployment != nil:
		writeJSON(w, status, res.Deployment)
	case res.Service != nil:
		writeJSON(w, status, res.Service)
	case res.Node != nil:
		writeJSON(w, status, res.Node)
	default:
		writeJSON(w, status, map[string]any{"revision": res.Revision})
	}
}

// listResponse wraps collections so the shape can gain pagination or a
// revision marker without breaking clients that already parse it.
type listResponse[T any] struct {
	Items []T `json:"items"`
	// Revision is the store's applied index at read time, so a client can tell
	// whether two lists it fetched are mutually consistent.
	Revision uint64 `json:"revision"`
	Total    int    `json:"total"`
}

func writeList[T any](w http.ResponseWriter, items []T, revision uint64) {
	if items == nil {
		items = []T{}
	}
	writeJSON(w, http.StatusOK, listResponse[T]{Items: items, Revision: revision, Total: len(items)})
}

// decodeBody reads and validates a JSON request body.
//
// DisallowUnknownFields is on deliberately: silently ignoring a misspelled
// field means an operator sets `replicas: 5` as `replica: 5` and watches
// nothing happen. Failing loudly is the kinder behaviour.
func decodeBody(w http.ResponseWriter, r *http.Request, maxBytes int64, dst any) bool {
	if ct := r.Header.Get("Content-Type"); ct != "" && !isJSONContentType(ct) {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type",
			"request body must be application/json")
		return false
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		var syntaxErr *json.SyntaxError
		var typeErr *json.UnmarshalTypeError

		switch {
		case errors.As(err, &maxErr):
			writeError(w, http.StatusRequestEntityTooLarge, "body_too_large",
				fmt.Sprintf("request body must not exceed %d bytes", maxBytes))
		case errors.As(err, &syntaxErr):
			writeError(w, http.StatusBadRequest, "malformed_json",
				fmt.Sprintf("malformed JSON at byte %d", syntaxErr.Offset))
		case errors.As(err, &typeErr):
			writeError(w, http.StatusBadRequest, "type_mismatch",
				fmt.Sprintf("field %q expects %s", typeErr.Field, typeErr.Type))
		case errors.Is(err, io.EOF):
			writeError(w, http.StatusBadRequest, "empty_body", "a request body is required")
		default:
			writeError(w, http.StatusBadRequest, "malformed_json", err.Error())
		}
		return false
	}

	// A second value in the body usually means a client concatenated requests
	// or sent JSON Lines; accepting only the first would silently drop work.
	if dec.More() {
		writeError(w, http.StatusBadRequest, "multiple_values",
			"request body must contain exactly one JSON value")
		return false
	}
	return true
}

func isJSONContentType(ct string) bool {
	for i := 0; i < len(ct); i++ {
		if ct[i] == ';' {
			ct = ct[:i]
			break
		}
	}
	switch trimSpace(ct) {
	case "application/json", "text/json", "application/vnd.orion+json":
		return true
	}
	return false
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}

// queryInt reads a bounded integer query parameter.
func queryInt(r *http.Request, key string, def, minV, maxV int) int {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	if n < minV {
		return minV
	}
	if n > maxV {
		return maxV
	}
	return n
}

func queryUint64(r *http.Request, key string) uint64 {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return 0
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func queryBool(r *http.Request, key string) bool {
	v, err := strconv.ParseBool(r.URL.Query().Get(key))
	return err == nil && v
}

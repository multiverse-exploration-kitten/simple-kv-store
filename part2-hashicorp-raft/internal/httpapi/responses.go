package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/hashicorp/raft"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// errorResponse is the body of every error answer.
type errorResponse struct {
	Error string `json:"error"`
	// Reason is set on 503 and 504.
	Reason    string `json:"reason,omitempty"`
	RetrySafe *bool  `json:"retry_safe,omitempty"`
	// RetryWithSameIdempotencyKey is set on a 504 of a write with an
	// Idempotency-Key: the FSM answers a retry with the first result.
	RetryWithSameIdempotencyKey bool    `json:"retry_with_same_idempotency_key,omitempty"`
	CurrentModifyIndex          *uint64 `json:"current_modify_index,omitempty"`
	Field                       string  `json:"field,omitempty"`
	Key                         string  `json:"key,omitempty"`
	Message                     string  `json:"message,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body) // the client may be gone; nothing to do then
}

// writeCASFailed: 409 with the current ModifyIndex, 0 when the key does not exist.
func writeCASFailed(w http.ResponseWriter, result node.WriteResult) {
	var currentModifyIndex uint64
	if result.Entry != nil {
		currentModifyIndex = result.Entry.ModifyIndex
	}
	writeJSON(w, http.StatusConflict, errorResponse{Error: "cas_failed", CurrentModifyIndex: &currentModifyIndex})
}

// writeError maps a node error to the HTTP answer.
func (server *api) writeError(w http.ResponseWriter, err error, failed operation) {
	var invalid *node.ValidationError
	var rejected *node.RejectedError
	var unknown *node.OutcomeUnknownError
	retrySafe, retryNotSafe := true, false
	switch {
	case errors.As(err, &invalid):
		status, code := http.StatusBadRequest, "invalid_request"
		if invalid.TooLarge {
			status, code = http.StatusRequestEntityTooLarge, "too_large"
		}
		writeJSON(w, status, errorResponse{Error: code, Field: invalid.Field, Message: invalid.Message})
	case errors.As(err, &rejected):
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "command_rejected", Reason: rejected.Reason, RetrySafe: &retrySafe})
	case errors.As(err, &unknown):
		writeJSON(w, http.StatusGatewayTimeout, errorResponse{
			Error:                       "outcome_unknown",
			Reason:                      unknown.Reason,
			RetrySafe:                   &retryNotSafe,
			RetryWithSameIdempotencyKey: failed.idempotencyKey != "",
		})
	case errors.Is(err, node.ErrIdempotencyKeyReused):
		writeJSON(w, http.StatusUnprocessableEntity, errorResponse{Error: "idempotency_key_reused", Message: err.Error()})
	case errors.Is(err, raft.ErrNothingNewToSnapshot):
		// raft has applied no command yet, so there is no state to save.
		writeJSON(w, http.StatusConflict, errorResponse{Error: "nothing_to_snapshot", Message: err.Error()})
	default:
		server.logger.Error("request failed with an unexpected error", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "internal_error", Message: err.Error()})
	}
}

// rejectionReason is the reason of a RejectedError, or the error text.
func rejectionReason(err error) string {
	var rejected *node.RejectedError
	if errors.As(err, &rejected) {
		return rejected.Reason
	}
	return err.Error()
}

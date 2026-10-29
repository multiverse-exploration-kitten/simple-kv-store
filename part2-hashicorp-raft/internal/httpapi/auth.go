package httpapi

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// requireAdminToken guards a call that changes the cluster or takes a
// snapshot. Without a configured token the call is off (fail closed). The
// check runs before a forward, and the forward keeps the Authorization
// header, so the leader checks the token again.
func (server *api) requireAdminToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if server.options.AdminToken == "" {
			writeJSON(w, http.StatusForbidden, errorResponse{
				Error:   "admin_disabled",
				Message: "this node has no admin token (KV_ADMIN_TOKEN), so the admin calls are off",
			})
			return
		}
		if !hasBearerToken(r, server.options.AdminToken) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeJSON(w, http.StatusUnauthorized, errorResponse{
				Error:   "unauthorized",
				Message: "send the header Authorization: Bearer <admin token>",
			})
			return
		}
		next(w, r)
	}
}

// hasBearerToken compares in constant time, so the answer time does not
// tell how much of a guessed token is right.
func hasBearerToken(r *http.Request, wantToken string) bool {
	scheme, token, found := strings.Cut(r.Header.Get("Authorization"), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(wantToken)) == 1
}

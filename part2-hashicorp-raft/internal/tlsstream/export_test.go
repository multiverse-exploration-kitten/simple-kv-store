package tlsstream

import "time"

// SetHandshakeTimeout gives the tests a short handshake timeout. Call it
// before the layer accepts or dials.
func (layer *StreamLayer) SetHandshakeTimeout(timeout time.Duration) {
	layer.handshakeTimeout = timeout
}

// Package remote provides a lightweight Remote Method Invocation (RMI)-style library
// built on top of TCP sockets.
//
// This file contains shared types and utilities used by both the caller-side and
// callee-side components of the library:
//
//   - LeakySocket: a wrapper around net.Conn that can emulate delay and packet loss.
//   - RemoteError: a library-specific error type used as the final return value for
//     all exported remote methods.
//   - RequestMsg / ReplyMsg: internal wire message structures for method calls.
//   - goEncode / goDecode: helper functions for gob-based serialization.
//
// The goal is to keep common protocol and transport utilities centralized so both
// stubs share the same definitions.
package remote

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"math/rand"
	"net"
	"time"
)

// ----------------------------- LeakySocket -----------------------------

// LeakySocket is a wrapper for a net.Conn connection that emulates
// transmission delays and random packet loss.
//
// The lab requires that all network I/O between caller and callee uses this wrapper
// (rather than writing directly to net.Conn). The wrapper provides:
//   - Send([]byte) (bool, error): possibly drops a message (returns ok=false) or delays it.
//   - Recv() ([]byte, error): blocks until bytes are received (or returns an error).
//
// Configuration knobs (enabled/disabled at runtime):
//   - Loss: probability-based dropping of messages
//   - Delay: sleep-based propagation delay before sending
//
// Note: This implementation uses a fixed-size read buffer in Recv (4096 bytes).
type LeakySocket struct {
	// Underlying TCP connection.
	s net.Conn

	// Loss configuration.
	isLossy  bool
	lossRate float32

	// Delay configuration.
	isDelayed bool

	// Timeout used to simulate "waiting" for an acknowledgement when a packet is dropped.
	msTimeout int
	usTimeout int

	// Artificial propagation delay before writes.
	msDelay int
	usDelay int
}

// NewLeakySocket constructs a LeakySocket wrapper for a given net.Conn.
//
// Parameters:
//   - conn: the underlying TCP connection to wrap
//   - lossy: whether to enable packet loss simulation
//   - delayed: whether to enable artificial propagation delay
//
// Default parameters:
//   - lossRate = 0.05 (5% drop probability)
//   - timeout  = 500ms
//   - delay    = 2ms
func NewLeakySocket(conn net.Conn, lossy bool, delayed bool) *LeakySocket {
	return &LeakySocket{
		s:         conn,
		isLossy:   lossy,
		lossRate:  0.05,
		isDelayed: delayed,
		msTimeout: 500,
		usTimeout: 0,
		msDelay:   2,
		usDelay:   0,
	}
}

// Send transmits a byte slice over the connection while emulating loss and delay.
//
// Behavior:
//   - If obj is nil, Send returns (true, nil) without writing.
//   - If loss is enabled and a random draw is below lossRate, Send simulates a drop by:
//     1) sleeping for the configured timeout, then
//     2) returning (false, nil)
//   - Otherwise, if delay is enabled, it sleeps for the configured delay and then writes.
//
// Returns:
//   - ok=true if the message was written (or obj was nil)
//   - ok=false if the message was dropped by the simulated network
//   - error non-nil if the underlying socket write fails or if the socket is nil
func (ls *LeakySocket) Send(obj []byte) (bool, error) {
	if obj == nil {
		return true, nil
	}

	if ls.s != nil {
		// Seed RNG for lossy behavior. (Note: re-seeding every call is deterministic enough
		// for this lab, though not generally recommended in production code.)
		rand.Seed(time.Now().UnixNano())

		// Simulate packet loss.
		if ls.isLossy && rand.Float32() < ls.lossRate {
			time.Sleep(time.Duration(ls.msTimeout)*time.Millisecond + time.Duration(ls.usTimeout)*time.Microsecond)
			return false, nil
		}

		// Simulate propagation delay.
		if ls.isDelayed {
			time.Sleep(time.Duration(ls.msDelay)*time.Millisecond + time.Duration(ls.usDelay)*time.Microsecond)
		}

		// Write bytes to underlying connection.
		_, err := ls.s.Write(obj)
		if err != nil {
			return false, errors.New("Send Write error: " + err.Error())
		}
		return true, nil
	}

	return false, errors.New("Send failed, nil socket")
}

// Recv receives a byte slice from the underlying TCP connection.
//
// Behavior:
//   - Allocates a fixed 4096-byte buffer and reads into it.
//   - Continues reading until it receives at least one byte.
//   - If it sees io.EOF, it continues looping (treating EOF as "no data yet").
//   - Returns a slice of the buffer containing exactly the bytes read.
//
// Returns:
//   - received bytes and nil error if read succeeds
//   - nil and an error if an unexpected read error occurs or socket is nil
func (ls *LeakySocket) Recv() ([]byte, error) {
	if ls.s != nil {
		buf := make([]byte, 4096)
		n := 0
		var err error

		for n <= 0 {
			n, err = ls.s.Read(buf)

			if n > 0 {
				return buf[:n], nil
			}

			// Ignore EOF (continue looping) but fail on other errors.
			if err != nil {
				if err != io.EOF {
					return nil, errors.New("Recv Read error: " + err.Error())
				}
			}
		}
	}

	return nil, errors.New("Recv failed, nil socket")
}

// SetDelay enables/disables delay simulation and updates the delay parameter.
//
// Parameters:
//   - delayed: enable/disable delay simulation
//   - ms/us: delay duration split into milliseconds and microseconds
func (ls *LeakySocket) SetDelay(delayed bool, ms int, us int) {
	ls.isDelayed = delayed
	ls.msDelay = ms
	ls.usDelay = us
}

// SetTimeout updates the timeout used when simulating packet loss.
//
// Parameters:
//   - ms/us: timeout duration split into milliseconds and microseconds
func (ls *LeakySocket) SetTimeout(ms int, us int) {
	ls.msTimeout = ms
	ls.usTimeout = us
}

// SetLossRate enables/disables packet loss simulation and sets the drop probability.
//
// Parameters:
//   - lossy: enable/disable loss simulation
//   - rate: probability in [0,1] that a packet is dropped
func (ls *LeakySocket) SetLossRate(lossy bool, rate float32) {
	ls.isLossy = lossy
	ls.lossRate = rate
}

// Close closes the underlying connection.
func (ls *LeakySocket) Close() error {
	return ls.s.Close()
}

// ----------------------------- RemoteError -----------------------------

// RemoteError is a library-specific error type.
//
// All remote methods in this lab must return RemoteError as their final return value.
// This error is reserved for stub-to-stub failures (encoding/decoding errors,
// connection failures, protocol mismatch, etc.), not application-level errors.
//
// Application-level errors can still be returned by the remote method as other return
// values (e.g., string, error-like types), but RemoteError communicates issues that
// occurred during the remote invocation itself.
type RemoteError struct {
	// Err stores the error message string. An empty string indicates success.
	Err string
}

// Error returns the embedded error string so RemoteError implements the error interface.
func (e *RemoteError) Error() string {
	return e.Err
}

// ----------------------------- Msg structs -----------------------------

// RequestMsg represents a caller-to-callee request message.
//
// Fields:
//   - Method: the remote method name (typically the service interface field name)
//   - Args: arguments to the remote method, represented as []any so it can carry
//     arbitrary gob-encodable types.
type RequestMsg struct {
	Method string
	Args   []any /// []reflect.Value
}

// ReplyMsg represents a callee-to-caller reply message.
//
// Fields:
//   - Success: indicates whether the callee successfully processed the request
//   - Reply: the returned values from the invoked method, excluding the final RemoteError
//     (RemoteError is carried separately in Err)
//   - Err: library RemoteError describing stub/protocol failures (Err.Err == "" means ok)
type ReplyMsg struct {
	Success bool
	Reply   []any /// []reflect.Value
	Err     RemoteError
}

// ----------------------------- Encode and Decode -----------------------------

// goEncode serializes v into a gob-encoded byte slice.
//
// This is used by both caller and callee to encode RequestMsg and ReplyMsg before
// sending them over the network.
func goEncode(v any) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(v)
	return buf.Bytes(), err
}

// goDecode deserializes gob-encoded data into v.
//
// v should be a pointer to the value that should be populated by decoding.
// This is used by both caller and callee after receiving bytes from the network.
func goDecode(data []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}

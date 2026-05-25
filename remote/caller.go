// Package remote provides a lightweight Remote Method Invocation (RMI)-style library
// implemented on top of TCP sockets.
//
// This file contains the caller-side implementation (the client-side stub builder).
// It is responsible for:
//
//   - Creating a generic RPC request/reply exchange over TCP using LeakySocket.
//   - Retrying calls when the simulated network drops packets.
//   - Using reflection to populate a user-provided "service interface" struct with
//     callable function implementations via reflect.MakeFunc.
//
// The main entry point is CallerStubCreator, which transforms a struct of function
// declarations into a working caller stub that forwards calls to a remote CalleeStub.
package remote

import (
	"errors"
	"net"
	"reflect"
	"time"
)

// makeFailureReturn constructs the return values for a stub method when the call fails.
//
// Behavior:
//   - For all non-RemoteError return values: returns the zero value for that type.
//   - For the last return value (RemoteError): returns a RemoteError containing msg.
//
// This helper is used inside the generated stub functions to ensure the caller always
// receives a correctly shaped result slice matching the original method signature.
func makeFailureReturn(funcType reflect.Type, msg string) []reflect.Value {
	nout := funcType.NumOut()
	out := make([]reflect.Value, nout)

	// Fill all non-RemoteError outputs with zero values so the caller gets a valid
	// set of return values even on failure.
	for i := 0; i < nout-1; i++ {
		out[i] = reflect.Zero(funcType.Out(i))
	}

	// Populate the last return value with the library-specific RemoteError.
	out[nout-1] = reflect.ValueOf(RemoteError{Err: msg})
	return out
}

// recvWithTimeout receives bytes from a LeakySocket with a hard timeout.
//
// Motivation:
// LeakySocket.Recv() can loop on io.EOF in certain situations, which can cause a
// goroutine to block indefinitely waiting for "real data". To protect callers from
// hanging forever, this helper runs Recv in a goroutine and applies a timeout.
//
// Parameters:
//   - ls: LeakySocket wrapper used for receiving
//   - conn: underlying net.Conn (closed on timeout to force unblock)
//   - timeout: maximum duration to wait for a receive
//
// Returns:
//   - received bytes and nil error on success
//   - nil bytes and an error on timeout or receive failure
func recvWithTimeout(ls *LeakySocket, conn net.Conn, timeout time.Duration) ([]byte, error) {
	// result bundles the outcome of ls.Recv() so we can return either bytes or an error.
	type result struct {
		b   []byte
		err error
	}

	// Buffered channel prevents goroutine leak if the select chooses the timeout case.
	ch := make(chan result, 1)

	// Perform the blocking Recv in a goroutine.
	go func() {
		b, err := ls.Recv()
		ch <- result{b: b, err: err}
	}()

	// Return whichever happens first: a recv result or a timeout.
	select {
	case res := <-ch:
		return res.b, res.err
	case <-time.After(timeout):
		// Force unblock: Recv may spin on EOF; closing the conn makes Read fail with a real error.
		_ = conn.Close()
		return nil, errors.New("recv timeout")
	}
}

// doRemoteCall executes a single logical remote method call over TCP.
//
// This function implements the caller-side request/reply protocol with retries to handle
// the simulated lossy/delayed network provided by LeakySocket.
//
// High-level flow per attempt:
//  1. Dial a fresh TCP connection to adr.
//  2. Wrap it in a LeakySocket configured using lossy/delayed.
//  3. Encode a RequestMsg containing the method name and arguments.
//  4. Send request via LeakySocket.Send(). If dropped, retry.
//  5. Receive the reply via recvWithTimeout(). If timeout/failure, retry.
//  6. Decode the ReplyMsg and return it.
//
// Retry strategy:
//   - Retries until overallDeadline is reached.
//   - Uses a small linear backoff (based on attempt count) between attempts.
//
// Returns:
//   - ReplyMsg and nil error on success
//   - empty ReplyMsg and error after exhausting retries / timing out
func doRemoteCall(method string, args []any, adr string, lossy bool, delayed bool) (ReplyMsg, error) {
	// lastErr captures the most recent failure so the final returned error contains context.
	var lastErr error

	// overallDeadline bounds the total time spent retrying this logical call.
	overallDeadline := time.Now().Add(2 * time.Second) // overall RPC deadline

	// perAttemptRecvTO bounds how long the client waits for a reply for a single attempt.
	perAttemptRecvTO := 400 * time.Millisecond // recv timeout per attempt

	// backoffBase is used to avoid tight retry loops under loss.
	backoffBase := 10 * time.Millisecond // small backoff

	for attempt := 0; time.Now().Before(overallDeadline); attempt++ {
		// Dial a fresh connection each attempt. This matches the lab's single-call-per-connection model.
		conn, err := net.DialTimeout("tcp", adr, 300*time.Millisecond)
		if err != nil {
			lastErr = err
			time.Sleep(backoffBase + time.Duration(attempt)*backoffBase)
			continue
		}

		// Wrap the TCP connection in a LeakySocket to emulate loss/delay.
		ls := NewLeakySocket(conn, lossy, delayed)

		// Encode request (method name + arguments).
		req := RequestMsg{Method: method, Args: args}
		b, err := goEncode(req)
		if err != nil {
			lastErr = err
			_ = ls.Close()
			time.Sleep(backoffBase + time.Duration(attempt)*backoffBase)
			continue
		}

		// Send request. If Send returns ok==false, the packet was dropped by the simulated network.
		ok, err := ls.Send(b)
		if err != nil {
			lastErr = err
			_ = ls.Close()
			time.Sleep(backoffBase + time.Duration(attempt)*backoffBase)
			continue
		}
		if !ok {
			// Dropped by leaky socket; retry on a new connection.
			lastErr = errors.New("request dropped")
			_ = ls.Close()
			time.Sleep(backoffBase + time.Duration(attempt)*backoffBase)
			continue
		}

		// Receive reply with a per-attempt timeout to avoid indefinite blocking.
		rb, err := recvWithTimeout(ls, conn, perAttemptRecvTO)
		if err != nil {
			lastErr = err
			_ = ls.Close()
			time.Sleep(backoffBase + time.Duration(attempt)*backoffBase)
			continue
		}

		// Decode ReplyMsg.
		var rep ReplyMsg
		if err := goDecode(rb, &rep); err != nil {
			lastErr = err
			_ = ls.Close()
			time.Sleep(backoffBase + time.Duration(attempt)*backoffBase)
			continue
		}

		// Cleanly close and return successful reply.
		_ = ls.Close()
		return rep, nil
	}

	// If the loop exits due to deadline, produce a meaningful error message.
	if lastErr == nil {
		lastErr = errors.New("timeout")
	}
	return ReplyMsg{}, errors.New("remote call failed: " + lastErr.Error())
}

// CallerStubCreator populates the function fields of a service interface struct to create
// a functional caller stub.
//
// Conceptually, the user passes a pointer to a struct type whose fields are function
// signatures describing the remote methods. CallerStubCreator uses reflection to replace
// each nil function field with a real function created using reflect.MakeFunc.
//
// Each generated function:
//   - Collects its arguments.
//   - Performs a remote call by sending a RequestMsg (method name + args).
//   - Receives and decodes a ReplyMsg.
//   - Converts the reply values to the expected return types.
//   - Returns the converted values plus a final RemoteError (empty if no library error).
//
// Validation performed:
//   - ifc must not be nil.
//   - ifc must be a pointer to a struct.
//   - every field in the struct must be a function.
//   - the last return value of each function must be RemoteError.
//   - each function field must be settable.
//
// Parameters:
//   - ifc: pointer to a struct of function declarations (the service interface)
//   - adr: callee address ("ip:port")
//   - lossy/delayed: configuration passed through to LeakySocket for this stub
func CallerStubCreator(ifc interface{}, adr string, lossy bool, delayed bool) error {
	// Reject nil early.
	if ifc == nil {
		return errors.New("function interface cannot be nil")
	}

	// Reflect type/value for validation and field setting.
	ifcType := reflect.TypeOf(ifc)
	ifcValue := reflect.ValueOf(ifc)
	ifcValueElem := ifcValue.Elem()
	ifcTypeElem := ifcType.Elem()

	// Validate that the interface is a pointer to a struct.
	if ifcType.Kind() != reflect.Ptr || ifcType.Elem().Kind() != reflect.Struct {
		return errors.New("function interface must be a pointer to a struct")
	}

	// Iterate through each method field in the service interface struct.
	for i := 0; i < ifcTypeElem.NumField(); i++ {
		// typeField describes the field's metadata (name, signature, etc.)
		typeField := ifcTypeElem.Field(i)

		// valueField is the actual field value we will set (it must be settable).
		valueField := ifcValueElem.Field(i)

		// Each field must be a function signature.
		if typeField.Type.Kind() != reflect.Func {
			return errors.New("function interface must only contain functions")
		}

		// Enforce the remote interface rule: last return must be RemoteError.
		if typeField.Type.NumOut() == 0 || typeField.Type.Out(typeField.Type.NumOut()-1) != reflect.TypeOf(RemoteError{}) {
			return errors.New("last return value of function interface methods must be RemoteError")
		}

		// The field must be settable, otherwise reflect.MakeFunc cannot be installed.
		if !valueField.CanSet() {
			return errors.New("Can not set function field")
		}

		// The method name is the struct field name (used as the wire "Method" identifier).
		methodName := typeField.Name

		// funcType is the full function signature (inputs + outputs).
		funcType := typeField.Type

		// handler is called by reflect.MakeFunc when the generated function is invoked.
		// It must return a []reflect.Value matching funcType.NumOut().
		handler := func(in []reflect.Value) []reflect.Value {
			// Convert input arguments to a serializable slice.
			args := make([]any, len(in))
			for k := range in {
				args[k] = in[k].Interface()
			}

			// Perform the network call (with retry logic inside doRemoteCall).
			reply, err := doRemoteCall(methodName, args, adr, lossy, delayed)
			if err != nil {
				// Library-level failure (transport/protocol) -> return zero values + RemoteError.
				return makeFailureReturn(funcType, "remote call failed "+err.Error())
			}
			if !reply.Success || reply.Err.Err != "" {
				// The callee indicated failure or included a RemoteError string.
				msg := reply.Err.Err
				if msg == "" {
					msg = "remote call failed"
				}
				return makeFailureReturn(funcType, msg)
			}

			// Convert reply values into the exact types expected by the caller.
			nout := funcType.NumOut()
			out := make([]reflect.Value, nout)

			// The wire format carries only non-RemoteError return values.
			// The final RemoteError is supplied by the stub layer.
			if len(reply.Reply) != nout-1 {
				return makeFailureReturn(funcType, "number of reply values are not same as non-RemoteError returns")
			}

			for j := 0; j < nout-1; j++ {
				rt := funcType.Out(j)

				// For interface-carried values, nil must be handled explicitly.
				if reply.Reply[j] == nil {
					out[j] = reflect.Zero(rt)
					continue
				}
				rv := reflect.ValueOf(reply.Reply[j])

				// Accept direct assignment or conversion; otherwise fail with RemoteError.
				if rv.IsValid() && rv.Type().AssignableTo(rt) {
					out[j] = rv
				} else if rv.IsValid() && rv.Type().ConvertibleTo(rt) {
					out[j] = rv.Convert(rt)
				} else {
					return makeFailureReturn(funcType, "reply type mismatch")
				}
			}

			// Last return value is the library RemoteError; empty indicates success.
			out[nout-1] = reflect.ValueOf(RemoteError{})
			return out
		}

		// Create the actual function value with the exact signature of the field.
		fn := reflect.MakeFunc(funcType, handler)

		// Install it into the struct so callers can invoke it like a normal method.
		valueField.Set(fn)
	}

	return nil
}

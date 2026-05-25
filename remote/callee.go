// Package remote provides a lightweight Remote Method Invocation (RMI)-style library
// implemented on top of TCP sockets.
//
// This file contains the callee-side implementation (the server-side stub).
// The CalleeStub hosts a single service object instance on a single TCP address.
// Callers connect, send a single request (method name + arguments), receive a single
// reply (return values or a RemoteError), and then disconnect.
//
// The implementation is designed to be generic: it uses reflection to locate and
// invoke methods by name on the hosted object instance.
//
// Note: This file relies on types and helpers defined elsewhere in the package:
//   - NewLeakySocket for lossy/delayed connections
//   - RequestMsg and ReplyMsg for the wire protocol
//   - goEncode/goDecode for gob-based serialization
//   - RemoteError for library-level error reporting
package remote

import (
	"errors"
	"net"
	"reflect"
	"sync"
)

// CalleeStub is the server-side stub that hosts a single remote object instance and
// exposes it over TCP.
//
// A CalleeStub encapsulates a multithreaded TCP server (one goroutine per connection).
// Each connection is treated as a single synchronous remote call: receive request,
// invoke the local method on the hosted object, and send the reply.
//
// The fields stored here capture:
//   - the service interface definition (type/value) used for validation
//   - the object instance value used for method invocation
//   - TCP server configuration and runtime state (listener/running flag)
//   - a mutex protecting shared state accessed concurrently
//   - a call counter used by tests and debugging
type CalleeStub struct {
	// ifcType is the reflected type of the service interface struct (the struct whose
	// fields are function signatures). This is used to validate requested method names
	// and ensure that the callee only serves methods declared in the interface.
	ifcType reflect.Type

	// ifcValue is the reflected value of the service interface struct. This is not
	// heavily used at runtime in this implementation, but is retained for completeness
	// and potential extensions (e.g., per-method signature checks).
	ifcValue reflect.Value

	// objValue is the reflected value of the hosted object instance. Incoming requests
	// locate methods by name on this value and invoke them via reflection.
	objValue reflect.Value

	// address is the TCP listening address for this callee (format: "ip:port").
	address string

	// isLossy indicates whether the callee should wrap accepted connections in a
	// LeakySocket configured to emulate packet loss.
	isLossy bool

	// isDelayed indicates whether the callee should wrap accepted connections in a
	// LeakySocket configured to emulate propagation delay.
	isDelayed bool

	// listener is the net.Listener created by Start(). It is closed by Stop().
	listener net.Listener

	// running is true while the server is accepting connections. It is protected by mu.
	running bool

	// mu guards concurrent access to mutable state: running, listener, and callCount.
	mu sync.Mutex

	// callCount tracks how many remote calls were successfully handled. It is incremented
	// after a reply is sent back to the caller.
	callCount int
}

// Callee defines the minimum contract our CalleeStub implementation must satisfy.
//
// The tests expect NewCalleeStub to return a value implementing this interface.
type Callee interface {
	// Start begins listening on the configured address. It returns once the listener
	// is created and the accept loop is launched.
	Start() error

	// Stop stops the server and closes the listening socket. It returns once the listener
	// is closed and the server has been marked as not running.
	Stop() error

	// IsRunning reports whether the callee is currently running and accepting connections.
	IsRunning() bool

	// GetCallCount returns the total number of handled calls across the lifetime of this stub.
	GetCallCount() int
}

/*
Start takes the validated CalleeStub and starts its TCP server.

Concurrency / safety notes:
  - A mutex is used to protect shared state (running/listener).
  - Start rejects duplicate starts to prevent binding the same port twice.
  - The accept loop is started in a separate goroutine so Start returns immediately.
*/
func (cs *CalleeStub) Start() error {
	// Read running under lock to avoid races with Stop().
	cs.mu.Lock()
	running := cs.running
	cs.mu.Unlock()

	// Prevent duplicate listeners on the same stub.
	if running {
		return errors.New("already running")
	}

	// Bind to the address. If the address is already in use, net.Listen returns an error.
	ln, err := net.Listen("tcp", cs.address)
	if err != nil {
		return err
	}

	// Store listener and mark running under lock so the accept loop can observe consistent state.
	cs.mu.Lock()
	cs.listener = ln
	cs.running = true
	cs.mu.Unlock()

	// Start accepting connections in the background.
	go cs.acceptLoop()
	return nil
}

/*
Stop shuts down the TCP server and releases the bound port.

Implementation notes:
  - Closing the listener unblocks any goroutine currently blocked in Accept().
  - The accept loop checks the running flag to decide whether to exit after Accept() fails.
*/
func (cs *CalleeStub) Stop() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Reject Stop if the server is already stopped.
	if !cs.running {
		return errors.New("not running")
	}

	// Mark not running first so the accept loop can exit cleanly.
	cs.running = false

	// Closing the listener causes Accept() to return an error.
	cs.listener.Close()
	return nil
}

/*
IsRunning returns true if the callee server has been started and has not been stopped.
*/
func (cs *CalleeStub) IsRunning() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.running
}

/*
GetCallCount returns the number of successfully handled calls.

A call is counted once the callee has invoked the target method and successfully sent
a reply back to the caller.
*/
func (cs *CalleeStub) GetCallCount() int {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.callCount
}

/*
acceptLoop continuously accepts new TCP connections while the server is running.

Each accepted connection is handled in its own goroutine to support concurrent clients.
If Accept returns an error:
  - If the server has been stopped (running == false), the loop exits.
  - Otherwise, it continues and retries Accept (e.g., transient network errors).
*/
func (cs *CalleeStub) acceptLoop() {
	for {
		// Check running status under lock to avoid races with Stop().
		cs.mu.Lock()
		if !cs.running {
			cs.mu.Unlock()
			return
		}
		ln := cs.listener
		cs.mu.Unlock()

		// Block until a client connects or the listener is closed.
		conn, err := ln.Accept()
		if err != nil {
			// If Stop() was called, exit; otherwise retry.
			cs.mu.Lock()
			running := cs.running
			cs.mu.Unlock()
			if !running {
				return
			}
			continue
		}

		// Handle each connection concurrently.
		go cs.handleConnection(conn)
	}
}

/*
handleConnection services exactly one request/reply exchange over a TCP connection.

High-level flow:
 1. Wrap the raw TCP connection in a LeakySocket (to emulate loss/delay if enabled).
 2. Receive and decode a RequestMsg (method name + arguments).
 3. Validate that the method is declared in the service interface.
 4. Locate and invoke the corresponding method on the hosted object using reflection.
 5. Encode and send a ReplyMsg containing the method's return values (excluding the final RemoteError).
 6. Increment callCount if a reply was successfully sent.

Error handling:
  - Whenever possible, this function attempts to send a failure ReplyMsg so that the
    caller does not block waiting for a reply.
*/
func (cs *CalleeStub) handleConnection(conn net.Conn) {
	// Wrap connection so tests can enable lossy/delayed behavior end-to-end.
	ls := NewLeakySocket(conn, cs.isLossy, cs.isDelayed)
	defer ls.Close()

	// sendFail attempts to send a failure reply over the connection.
	// This helps prevent client-side blocking when the server rejects a request.
	sendFail := func(msg string) {
		rep := ReplyMsg{
			Success: false,
			Reply:   nil,
			Err:     RemoteError{Err: msg},
		}
		if b, err := goEncode(rep); err == nil {
			// Ignore send failure because the connection may already be broken or dropped.
			_, _ = ls.Send(b)
		}
	}

	// Receive raw bytes of the request.
	data, err := ls.Recv()
	if err != nil {
		// If we cannot read the request at all, there is nothing meaningful to send back.
		return
	}

	// Decode RequestMsg (method name + args).
	var req RequestMsg
	if err := goDecode(data, &req); err != nil {
		sendFail("decode request failed: " + err.Error())
		return
	}

	// Ensure the requested method is part of the declared service interface.
	// This prevents a caller from invoking arbitrary methods on the object instance.
	_, ok := cs.ifcType.FieldByName(req.Method)
	if !ok {
		sendFail("method is not part of service interface")
		return
	}

	// Locate the method on the hosted object instance by name.
	method := cs.objValue.MethodByName(req.Method)
	if !method.IsValid() {
		sendFail("unknown method: " + req.Method)
		return
	}

	// Validate and convert request arguments to the exact parameter types expected
	// by the reflected method signature.
	nargs := method.Type().NumIn()
	if len(req.Args) != nargs {
		sendFail("argument count mismatch")
		return
	}

	// Build the reflect.Value slice to pass to method.Call().
	inVals := make([]reflect.Value, nargs)
	for i := 0; i < nargs; i++ {
		exptectedArgType := method.Type().In(i)

		actualArgType := reflect.ValueOf(req.Args[i])
		if !actualArgType.IsValid() {
			sendFail("invalid argument")
			return
		}

		// Permit arguments if they are directly assignable or convertible.
		// Otherwise reject the call to avoid panics and type confusion.
		if actualArgType.Type().AssignableTo(exptectedArgType) {
			inVals[i] = actualArgType
		} else if actualArgType.Type().ConvertibleTo(exptectedArgType) {
			inVals[i] = actualArgType.Convert(exptectedArgType)
		} else {
			sendFail("argument type mismatch")
			return
		}
	}

	// Invoke the method on the hosted object.
	outs := method.Call(inVals)

	// Verify that the method returns at least one value and ends with RemoteError.
	// This is the lab's "remote method" signature constraint.
	nout := method.Type().NumOut()
	if nout == 0 || method.Type().Out(nout-1) != reflect.TypeOf(RemoteError{}) {
		sendFail("invalid method signature: last return must be RemoteError")
		return
	}

	// Build ReplyMsg. This implementation sends all return values except the last RemoteError.
	// The library-level error channel is ReplyMsg.Err instead.
	if len(outs) == 0 {
		sendFail("method returned no values")
		return
	}

	rep := ReplyMsg{
		Success: true,
		Reply:   make([]any, len(outs)-1),
		Err:     RemoteError{},
	}
	for j := 0; j < len(outs)-1; j++ {
		rep.Reply[j] = outs[j].Interface()
	}

	// Encode reply using gob and transmit back to the caller.
	b, err := goEncode(rep)
	if err != nil {
		sendFail("encode reply failed: " + err.Error())
		return
	}

	// Send reply. If Send fails (socket error) or is dropped (loss simulation),
	// we do not retry here; the caller-side logic may retry using a new connection.
	if ok, err := ls.Send(b); err != nil {
		return
	} else if !ok {
		return
	}

	// Count the call only after we have successfully sent a reply.
	cs.mu.Lock()
	cs.callCount++
	cs.mu.Unlock()
}

// NewCalleeStub builds a new CalleeStub instance around a given service interface,
// a concrete service object that implements the interface methods, and connection
// settings for LeakySocket behavior.
//
// Validation performed here matches the lab requirements:
//   - sv and sobj must not be nil
//   - sv must be a pointer to a struct
//   - every field in sv must be a function
//   - the last return type of each function must be RemoteError
//
// Parameters:
//   - sv: pointer to a struct whose fields are function signatures describing the service
//   - sobj: the concrete server-side object instance to host (methods invoked via reflection)
//   - address: TCP listen address ("ip:port")
//   - lossy: enable simulated loss on accepted connections
//   - delayed: enable simulated delay on accepted connections
func NewCalleeStub(sv interface{}, sobj interface{}, address string, lossy bool, delayed bool) (Callee, error) {
	// Reject nil inputs early.
	if sv == nil || sobj == nil {
		return nil, errors.New("service interface or object cannot be nil")
	}

	// Ensure service interface is a pointer to a struct.
	sType := reflect.TypeOf(sv)
	if sType.Kind() != reflect.Ptr || sType.Elem().Kind() != reflect.Struct {
		return nil, errors.New("service interface must be a pointer to a struct")
	}

	// Validate that all fields are functions and follow the remote signature rule.
	sTypeElem := sType.Elem()
	for i := 0; i < sTypeElem.NumField(); i++ {
		field := sTypeElem.Field(i)
		if field.Type.Kind() != reflect.Func {
			return nil, errors.New("service interface must only contain functions")
		}
		if field.Type.NumOut() == 0 || field.Type.Out(field.Type.NumOut()-1) != reflect.TypeOf(RemoteError{}) {
			return nil, errors.New("last return value of service interface methods must be RemoteError")
		}
	}

	// Construct the stub with validated metadata and runtime configuration.
	cs := &CalleeStub{
		ifcType:   sTypeElem,
		ifcValue:  reflect.ValueOf(sv).Elem(),
		objValue:  reflect.ValueOf(sobj),
		address:   address,
		isLossy:   lossy,
		isDelayed: delayed,
	}
	return cs, nil
}

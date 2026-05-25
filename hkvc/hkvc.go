// Package hkvc implements a hierarchical key-value cluster (HKVC) on top of
// the custom Raft and remote RPC libraries used in the lab.
//
// # High-level design
//
// The implementation separates cluster state into two layers:
//
//  1. Directory topology
//     The global hierarchy of directories is coordinated through base Raft group 0.
//     This includes creating and deleting directories, routing from a path to the
//     group that owns that directory, and serving metadata for directories.
//
//  2. Per-directory key/value contents
//     The ordinary keys stored inside a directory are coordinated through the Raft
//     group assigned to that directory. This includes list/get/set/delete of keys
//     within that directory and metadata for ordinary keys.
//
// # Client-facing behavior
//
// Each participant exposes a single HTTP server that multiplexes all HKVC client
// endpoints:
//
//   - /list
//   - /get_metadata
//   - /get
//   - /set
//   - /create
//   - /delete
//
// The server also exposes a few internal-only helper endpoints used by other
// participants to forward routing and commit requests. These endpoints are not part
// of the public client API, but they are useful for allowing a participant to find
// the correct leader for a group or to ask another participant to submit a command
// to that group's Raft log.
//
// # Consistency model
//
// All client requests that observe or modify replicated state are mapped to Raft
// commands and are answered only after the relevant command has committed and been
// applied locally. The implementation also tracks client sequence numbers per group
// so that:
//
//   - repeated commands with the same sequence number return the original response
//   - strictly older commands are rejected as out-of-sequence
//   - duplicate in-flight commands attach to the pending request rather than being
//     appended to the Raft log again
//
// # Storage model
//
// Directories are represented by DirNode values arranged as a tree rooted at “/”.
// Key/value contents for each directory are kept in dirData[path], where path is the
// normalized directory name. This design keeps directory topology and directory-local
// key/value state easy to reason about while still allowing multiple Raft groups to
// participate in ownership.
//
// This file is intentionally self-contained so the generated Go package
// documentation remains readable without forcing the reader to jump across multiple
// source files.
package hkvc

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"raft"
	"remote"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// init registers the message types that may cross goroutine, gob, or helper
// endpoint boundaries in this package.
//
// The Raft layer replicates opaque byte slices, but the HKVC implementation
// serializes concrete command structures into those byte slices. Registering them
// here keeps gob usage predictable across tests and helper calls.
func init() {
	gob.Register(HKVCStatusReport{})
	gob.Register(ClusterCommand{})
	gob.Register(internalRouteResponse{})
	gob.Register(internalLeaderStatus{})
	gob.Register(internalGroupRequest{})
	gob.Register(internalCommitRequest{})
	gob.Register(internalDirExistsRequest{})
	gob.Register(internalDirExistsResponse{})
}

// DirectoryRequest is the client request body for the /list endpoint.
//
// The directory path is interpreted using Unix-like slash-separated path tokens.
// SeqNumber and ClientID are used for idempotence and ordering checks.
type DirectoryRequest struct {
	Directory string `json:"directory"`
	SeqNumber int    `json:"seq_number"`
	ClientID  string `json:"client_id"`
}

// KeyRequest is the client request body for endpoints that identify a directory
// and one immediate child name inside that directory.
//
// It is used by:
//
//   - /get_metadata
//   - /get
//   - /create
//   - /delete
type KeyRequest struct {
	Directory string `json:"directory"`
	Key       string `json:"key"`
	SeqNumber int    `json:"seq_number"`
	ClientID  string `json:"client_id"`
}

// KeyValueMessage is used both as a /set request and as a /get response.
//
// The Value field is treated as an opaque string payload by this implementation.
type KeyValueMessage struct {
	Directory string `json:"directory"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	SeqNumber int    `json:"seq_number"`
	ClientID  string `json:"client_id"`
}

// ListResponse is the successful response body for /list.
//
// The List slice contains both immediate subdirectory names and immediate key
// names, sorted lexicographically.
type ListResponse struct {
	Directory string   `json:"directory"`
	List      []string `json:"list"`
	ClientID  string   `json:"client_id"`
}

// KeySuccessResponse is the successful response body for write-style endpoints
// that do not need to return a value payload.
//
// The Success field distinguishes cases like:
//   - newly created directory or key
//   - duplicate-safe create that found the directory already present
type KeySuccessResponse struct {
	Directory string `json:"directory"`
	Key       string `json:"key"`
	Success   bool   `json:"success"`
	ClientID  string `json:"client_id"`
}

// MetadataResponse is the successful response body for /get_metadata.
//
// For directories, Size is -1.
// For ordinary keys, Size is the length of the stored string payload.
type MetadataResponse struct {
	Directory   string   `json:"directory"`
	Key         string   `json:"key"`
	IsDirectory bool     `json:"is_directory"`
	Size        int      `json:"size"`
	Version     uint64   `json:"version"`
	PAddrList   []string `json:"p_addr_list"`
	LeaderIdx   int      `json:"leader_index"`
	Tags        []string `json:"tags"`
	ClientID    string   `json:"client_id"`
}

// HKVCErrorResponse is the common JSON error body returned by all public
// endpoints when a request cannot be completed successfully.
type HKVCErrorResponse struct {
	ErrorType string `json:"error_type"`
	ErrorInfo string `json:"error_info"`
	ClientID  string `json:"client_id"`
}

// Public error identifiers used throughout the client API.
//
// The exact names are part of the observable API and therefore intentionally
// centralized here.
const (
	// InvalidError is returned for malformed requests, unsupported methods,
	// invalid directory strings, invalid keys, and other basic client-side
	// request errors.
	InvalidError string = "HKVCInvalidRequestError"

	// DirNotFoundError indicates that the requested directory does not exist.
	DirNotFoundError string = "HKVCDirectoryNotFoundError"

	// KeyNotFoundError indicates that the requested key or directory child does
	// not exist immediately within the specified directory.
	KeyNotFoundError string = "HKVCKeyNotFoundError"

	// ConflictKeyError indicates that a path component or target name conflicts
	// with an existing ordinary key.
	ConflictKeyError string = "HKVCConflictExistingKeyError"

	// ConflictDirError indicates that a target key name conflicts with an
	// existing directory.
	ConflictDirError string = "HKVCConflictExistingDirectoryError"

	// NonLeaderError indicates that the receiving participant is not the leader
	// for the relevant Raft group and therefore cannot accept the command.
	NonLeaderError string = "HKVCNonRaftLeaderError"

	// OutOfSequenceError indicates that the request carries a sequence number
	// strictly smaller than the largest one already processed for that client in
	// the relevant group.
	OutOfSequenceError string = "HKVCMsgOutOfSequenceError"
)

// HKVCSetupInfo bundles the static configuration provided to each participant by
// the test controller at startup.
//
// Each participant receives the full cluster setup slice, not just its own entry.
type HKVCSetupInfo struct {
	// Id is the stable identifier assigned by the controller.
	Id int

	// ControlAddr is the remote-control address used by the test harness.
	ControlAddr string

	// ClientAddr is the address of the participant's public HTTP interface.
	ClientAddr string

	// RaftAddrs maps each group identifier to the Raft RPC address this
	// participant should use for that group.
	RaftAddrs map[int]string
}

// HKVCStatusReport is returned to the test controller through the control
// interface.
//
// The report is intentionally compact: it says whether the participant is active,
// which groups it currently leads, and the leader-visible commit index for each
// group.
type HKVCStatusReport struct {
	Active      bool
	GroupLeader map[int]bool
	GroupCommit map[int]int
}

// HKVCControlInterface is the remote control surface used by the tests.
//
// The implementation follows the same pattern as the earlier Raft lab: the
// controller creates a caller stub for this interface and invokes these methods
// to activate, deactivate, terminate, and inspect a participant.
type HKVCControlInterface struct {
	Activate   func() remote.RemoteError
	Deactivate func() remote.RemoteError
	Terminate  func() remote.RemoteError
	GetStatus  func() (HKVCStatusReport, remote.RemoteError)
}

// ClusterCommand is the internal replicated command format written into Raft.
//
// The public HTTP request structs are converted into this single command type
// before submission to the relevant group. The extra routing flags allow the
// apply logic to distinguish:
//
//   - public endpoint type
//   - owning group
//   - whether an operation refers to a directory object or a normal key
type ClusterCommand struct {
	Endpoint         string `json:"endpoint"`
	Directory        string `json:"directory"`
	Key              string `json:"key"`
	Value            string `json:"value"`
	ClientID         string `json:"client_id"`
	SeqNumber        int    `json:"seq_number"`
	RouteGroup       int    `json:"route_group"`
	AffectsDirectory bool   `json:"affects_directory"`
}

// StoredResponse is the internal canonical response representation.
//
// Public endpoints convert this structure into one of the client-visible JSON
// response types. Storing responses in this normalized form makes client retry
// handling straightforward because the original response can be replayed without
// reconstructing it from scratch.
type StoredResponse struct {
	Kind        string   `json:"kind"`
	Status      int      `json:"status"`
	ErrorType   string   `json:"error_type,omitempty"`
	ErrorInfo   string   `json:"error_info,omitempty"`
	Directory   string   `json:"directory,omitempty"`
	Key         string   `json:"key,omitempty"`
	Value       string   `json:"value,omitempty"`
	List        []string `json:"list,omitempty"`
	Success     bool     `json:"success,omitempty"`
	IsDirectory bool     `json:"is_directory,omitempty"`
	Size        int      `json:"size,omitempty"`
	Version     uint64   `json:"version,omitempty"`
	PAddrList   []string `json:"p_addr_list,omitempty"`
	LeaderIdx   int      `json:"leader_index,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	ClientID    string   `json:"client_id,omitempty"`
}

// clientHistory stores the latest processed sequence number and corresponding
// response for one client within one Raft group.
type clientHistory struct {
	LastSeq  int
	Response StoredResponse
}

// KeyRecord is the in-memory representation of one ordinary key stored inside a
// directory.
type KeyRecord struct {
	Value   string
	Version uint64
	Tags    []string
}

// DirNode is the in-memory representation of one directory in the global
// hierarchy.
//
// The directory tree is maintained through group 0. Each directory records the
// Raft group that owns the ordinary key/value contents of that directory.
type DirNode struct {
	// Name is the final path token for this directory.
	Name string

	// GroupID is the Raft group responsible for this directory's local key/value
	// contents.
	GroupID int

	// Version tracks logical updates to the directory metadata itself.
	Version uint64

	// Tags stores optional labels associated with the directory.
	Tags []string

	// Subdirs holds immediate child directories by child name.
	Subdirs map[string]*DirNode

	// Parent points at the containing directory, or nil for the root.
	Parent *DirNode

	// FullPath caches the normalized absolute path for convenience.
	FullPath string
}

// dirLookup is a helper result returned when resolving a directory path.
//
// It captures more detail than a simple "*DirNode or nil" so callers can
// distinguish:
//
//   - the directory exists
//   - the directory is missing
//   - the path attempted to descend through an ordinary key
type dirLookup struct {
	Node      *DirNode
	LastDir   *DirNode
	KeyInPath bool
	Missing   bool
}

// internalRouteResponse is returned by the internal routing helper endpoint.
//
// It tells a participant which group currently owns the requested directory.
type internalRouteResponse struct {
	GroupID int `json:"group_id"`
}

// internalLeaderStatus is returned by the internal leader status helper
// endpoint.
type internalLeaderStatus struct {
	Leader bool `json:"leader"`
	Active bool `json:"active"`
}

// internalGroupRequest is a small helper request body used by several internal
// endpoints that operate on one specific Raft group.
type internalGroupRequest struct {
	GroupID int `json:"group_id"`
}

// internalCommitRequest asks another participant to submit a command to the
// specified Raft group if it is currently that group's leader.
type internalCommitRequest struct {
	GroupID int            `json:"group_id"`
	Command ClusterCommand `json:"command"`
}

// internalDirExistsRequest is used by the internal directory-visibility helper
// endpoint.
type internalDirExistsRequest struct {
	Directory string `json:"directory"`
}

// internalDirExistsResponse indicates whether a directory is currently visible
// in the receiver's local view of the hierarchy.
type internalDirExistsResponse struct {
	Exists bool `json:"exists"`
}

// HKVCParticipant is one node in the hierarchical key-value cluster.
//
// A participant simultaneously hosts:
//
//   - zero or more local Raft peers, one per group that includes this node
//   - one remote control stub for the test harness
//   - one HTTP client interface for public HKVC requests
//   - local replicated state for directory topology and directory-owned keys
//
// # Locking policy
//
// The mutex protects the entire replicated HKVC state held in memory, including
// the directory tree, per-directory key/value maps, and the deduplication
// structures used for client retries. Helper functions suffixed with "Locked"
// assume the mutex is already held by the caller.
type HKVCParticipant struct {
	// mu protects all mutable HKVC state in this participant.
	mu sync.Mutex

	// active tracks whether the participant's public HTTP interface is currently
	// enabled. This is separate from individual Raft peer activation.
	active bool

	// info is the controller-provided setup slice for the full cluster.
	info []HKVCSetupInfo

	// myIndex is this participant's index within info.
	myIndex int

	// groups maps group identifiers to the cluster participant indices that
	// belong to that group.
	groups map[int][]int

	// sortedGroupID holds the group identifiers in sorted order so group
	// assignment is deterministic and repeatable.
	sortedGroupID []int

	// raftNodes contains the local Raft peer for each group that includes this
	// participant.
	raftNodes map[int]*raft.RaftNode

	// applyChs contains the apply channel corresponding to each local Raft peer.
	applyChs map[int]chan raft.ApplyMsg

	// rootDir is the root of the global directory hierarchy.
	rootDir *DirNode

	// dirData stores ordinary key/value data for each directory path.
	//
	// Conceptually:
	//   dirData[path][key] = key record
	dirData map[string]map[string]*KeyRecord

	// clientHistory holds per-group sequence tracking and replayable responses.
	clientHistory map[int]map[string]clientHistory

	// pendingReqs holds wait channels for in-flight commands keyed by
	// (group, client, seq). Duplicate in-flight requests attach to the same entry.
	pendingReqs map[int]map[string][]chan StoredResponse

	// nextGroupCursor is used to assign newly created directories to groups in a
	// simple round-robin fashion.
	nextGroupCursor int

	// controlStub exposes the test harness control interface.
	controlStub remote.Callee

	// httpHandler is the mux containing all public and internal HTTP endpoints.
	httpHandler http.Handler

	// httpServer is the public HTTP server for client and internal helper
	// requests.
	httpServer *http.Server

	// listener is the network listener currently backing httpServer.
	listener net.Listener
}

// NewHKVCParticipant constructs one HKVC participant.
//
// # Startup sequence
//
// The constructor:
//
//   - initializes local in-memory state
//   - creates a local Raft peer for each group that includes this participant
//   - launches one apply loop per local Raft group
//   - creates the remote control stub used by the test harness
//   - constructs the HTTP mux for both public and internal endpoints
//
// The participant is created with its control stub already running, but the
// public HTTP interface and local Raft peers are only activated later through
// Activate.
func NewHKVCParticipant(pInfo []HKVCSetupInfo, index int, groups map[int][]int) {
	p := &HKVCParticipant{
		info:          pInfo,
		myIndex:       index,
		groups:        groups,
		raftNodes:     make(map[int]*raft.RaftNode),
		applyChs:      make(map[int]chan raft.ApplyMsg),
		dirData:       make(map[string]map[string]*KeyRecord),
		clientHistory: make(map[int]map[string]clientHistory),
		pendingReqs:   make(map[int]map[string][]chan StoredResponse),
		rootDir: &DirNode{
			Name:     "/",
			GroupID:  0,
			Version:  1,
			Tags:     []string{},
			Subdirs:  make(map[string]*DirNode),
			Parent:   nil,
			FullPath: "/",
		},
	}

	// Root directory always exists and always has a key map, even if empty.
	p.dirData["/"] = make(map[string]*KeyRecord)

	// Pre-initialize per-group bookkeeping maps.
	for gid := range groups {
		p.sortedGroupID = append(p.sortedGroupID, gid)
		p.clientHistory[gid] = make(map[string]clientHistory)
		p.pendingReqs[gid] = make(map[string][]chan StoredResponse)
	}
	sort.Ints(p.sortedGroupID)

	// Create local Raft peers only for groups that include this participant.
	for gid, members := range groups {
		inGroup := false
		myRaftIndex := -1
		raftInfo := make([]raft.RaftSetupInfo, 0, len(members))
		for idx, memberIdx := range members {
			if memberIdx == index {
				inGroup = true
				myRaftIndex = idx
			}
			raftInfo = append(raftInfo, raft.RaftSetupInfo{
				Id:   pInfo[memberIdx].Id,
				Addr: pInfo[memberIdx].RaftAddrs[gid],
			})
		}
		if !inGroup {
			continue
		}

		ch := make(chan raft.ApplyMsg, 256)
		node := raft.NewRaftPeer(raftInfo, myRaftIndex, ch)
		p.raftNodes[gid] = node
		p.applyChs[gid] = ch
		go p.applyLoop(gid, ch)
	}

	var err error
	p.controlStub, err = remote.NewCalleeStub(&HKVCControlInterface{}, p, pInfo[index].ControlAddr, false, false)
	if err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/list", p.handleList)
	mux.HandleFunc("/get_metadata", p.handleGetMetadata)
	mux.HandleFunc("/get", p.handleGet)
	mux.HandleFunc("/set", p.handleSet)
	mux.HandleFunc("/create", p.handleCreate)
	mux.HandleFunc("/delete", p.handleDelete)

	// Internal-only coordination helpers used between participants.
	mux.HandleFunc("/_hkvc_internal/route", p.handleInternalRoute)
	mux.HandleFunc("/_hkvc_internal/commit_group", p.handleInternalCommitGroup)
	mux.HandleFunc("/_hkvc_internal/leader_status", p.handleInternalLeaderStatus)
	mux.HandleFunc("/_hkvc_internal/dir_exists", p.handleInternalDirExists)

	p.httpHandler = mux
	p.httpServer = &http.Server{
		Addr:    pInfo[index].ClientAddr,
		Handler: mux,
	}

	if err := p.controlStub.Start(); err != nil {
		panic(err)
	}
}

// parsePath splits a slash-separated path into non-empty tokens.
//
// Examples:
//
//   - "/"        -> []
//   - "/a/b"     -> ["a", "b"]
//   - "////a//b" -> ["a", "b"]
func parsePath(path string) []string {
	parts := strings.Split(path, "/")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// normalizeDirectory validates and normalizes a directory path.
//
// Rules implemented here:
//
//   - the string must be non-empty
//   - it must begin with '/'
//   - ':' is rejected because participant addresses use ip:port formatting and
//     the lab specification excludes such path forms
//   - repeated slashes collapse naturally
//   - a trailing slash has no effect
//
// The root directory normalizes to "/".
func normalizeDirectory(dir string) (string, bool) {
	if dir == "" || !strings.HasPrefix(dir, "/") || strings.Contains(dir, ":") {
		return "", false
	}
	parts := parsePath(dir)
	if len(parts) == 0 {
		return "/", true
	}
	return "/" + strings.Join(parts, "/"), true
}

// isValidKey validates an ordinary key or child directory name.
//
// Keys must be non-empty and cannot contain slash or colon characters.
func isValidKey(key string) bool {
	return key != "" && !strings.Contains(key, "/") && !strings.Contains(key, ":")
}

// isValidMetadataKey validates the key field accepted by /get_metadata.
//
// In addition to ordinary child names, the metadata endpoint accepts "." to
// refer to the directory named in the Directory field itself.
func isValidMetadataKey(key string) bool {
	return key == "." || isValidKey(key)
}

// opKey constructs the internal deduplication key for one client sequence
// number.
func opKey(clientID string, seq int) string {
	return clientID + "_" + strconv.Itoa(seq)
}

// cloneStrings returns a shallow copy of the input slice.
//
// The helper normalizes nil/empty handling so response construction stays
// straightforward and side-effect free.
func cloneStrings(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

// joinPath appends a single child token to a normalized directory path.
func joinPath(dir, key string) string {
	if dir == "/" {
		return "/" + key
	}
	return dir + "/" + key
}

// hasPathPrefix reports whether path is equal to prefix or lies strictly inside
// prefix as a directory subtree.
func hasPathPrefix(path, prefix string) bool {
	if prefix == "/" {
		return true
	}
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

// writeStoredResponse converts an internal StoredResponse into the concrete
// client-visible JSON response body and HTTP status code.
//
// This function is the only place where the normalized internal response format
// is translated back into the public API schema.
func (p *HKVCParticipant) writeStoredResponse(w http.ResponseWriter, resp StoredResponse) {
	if resp.Status == 0 {
		resp = StoredResponse{
			Kind:      "error",
			Status:    http.StatusInternalServerError,
			ErrorType: InvalidError,
			ErrorInfo: "internal error",
		}
	}

	w.WriteHeader(resp.Status)
	switch resp.Kind {
	case "error":
		_ = json.NewEncoder(w).Encode(HKVCErrorResponse{
			ErrorType: resp.ErrorType,
			ErrorInfo: resp.ErrorInfo,
			ClientID:  resp.ClientID,
		})
	case "list":
		_ = json.NewEncoder(w).Encode(ListResponse{
			Directory: resp.Directory,
			List:      cloneStrings(resp.List),
			ClientID:  resp.ClientID,
		})
	case "key_value":
		_ = json.NewEncoder(w).Encode(KeyValueMessage{
			Directory: resp.Directory,
			Key:       resp.Key,
			Value:     resp.Value,
			ClientID:  resp.ClientID,
		})
	case "key_success":
		_ = json.NewEncoder(w).Encode(KeySuccessResponse{
			Directory: resp.Directory,
			Key:       resp.Key,
			Success:   resp.Success,
			ClientID:  resp.ClientID,
		})
	case "metadata":
		_ = json.NewEncoder(w).Encode(MetadataResponse{
			Directory:   resp.Directory,
			Key:         resp.Key,
			IsDirectory: resp.IsDirectory,
			Size:        resp.Size,
			Version:     resp.Version,
			PAddrList:   cloneStrings(resp.PAddrList),
			LeaderIdx:   resp.LeaderIdx,
			Tags:        cloneStrings(resp.Tags),
			ClientID:    resp.ClientID,
		})
	default:
		_ = json.NewEncoder(w).Encode(HKVCErrorResponse{
			ErrorType: InvalidError,
			ErrorInfo: "unknown response kind",
			ClientID:  resp.ClientID,
		})
	}
}

// errorResponse is a convenience constructor for internal error responses.
func errorResponse(clientID, errType, errInfo string, status int) StoredResponse {
	return StoredResponse{
		Kind:      "error",
		Status:    status,
		ErrorType: errType,
		ErrorInfo: errInfo,
		ClientID:  clientID,
	}
}

// lookupDirectoryLocked resolves a normalized directory path against the
// in-memory directory tree.
//
// The caller must already hold p.mu.
//
// The result captures whether:
//
//   - the directory exists
//   - the path is missing below an existing directory
//   - the traversal attempted to step through a normal key instead of a
//     directory
func (p *HKVCParticipant) lookupDirectoryLocked(dir string) dirLookup {
	if dir == "/" {
		return dirLookup{Node: p.rootDir, LastDir: p.rootDir}
	}

	cur := p.rootDir
	curPath := "/"
	parts := parsePath(dir)

	for _, part := range parts {
		child, ok := cur.Subdirs[part]
		if ok {
			cur = child
			curPath = child.FullPath
			continue
		}

		// If the token is not a subdirectory but exists as a key in the current
		// directory, the path conflicts with an ordinary key.
		if kvs, ok2 := p.dirData[curPath]; ok2 {
			if _, existsAsKey := kvs[part]; existsAsKey {
				return dirLookup{LastDir: cur, KeyInPath: true}
			}
		}

		return dirLookup{LastDir: cur, Missing: true}
	}

	return dirLookup{Node: cur, LastDir: cur}
}

// routeGroupLocked determines the group responsible for the given directory or
// nearest existing ancestor.
//
// The caller must already hold p.mu.
func (p *HKVCParticipant) routeGroupLocked(dir string) int {
	lookup := p.lookupDirectoryLocked(dir)
	if lookup.Node != nil {
		return lookup.Node.GroupID
	}
	if lookup.LastDir != nil {
		return lookup.LastDir.GroupID
	}
	return 0
}

// nextAssignedGroupLocked selects the group that should own the contents of the
// next newly created directory.
//
// The current implementation uses a simple round-robin policy over the sorted
// list of known groups.
func (p *HKVCParticipant) nextAssignedGroupLocked() int {
	if len(p.sortedGroupID) == 0 {
		return 0
	}
	gid := p.sortedGroupID[p.nextGroupCursor%len(p.sortedGroupID)]
	p.nextGroupCursor++
	return gid
}

// groupAddressList returns the public client addresses of all participants in
// the specified group, preserving the controller-provided member order.
func (p *HKVCParticipant) groupAddressList(gid int) []string {
	members := p.groups[gid]
	out := make([]string, 0, len(members))
	for _, idx := range members {
		out = append(out, p.info[idx].ClientAddr)
	}
	return out
}

// isLeader reports whether this participant currently believes it is the active
// leader of the specified group.
func (p *HKVCParticipant) isLeader(gid int) bool {
	node, ok := p.raftNodes[gid]
	if !ok {
		return false
	}
	status, _ := node.GetStatus()
	return status.Active && status.Leader
}

// internalPost sends one JSON request to another participant's internal helper
// endpoint and decodes the JSON response into out when provided.
//
// The helper is intentionally tiny because all internal endpoints use simple
// POST-plus-JSON semantics.
func (p *HKVCParticipant) internalPost(addr, path string, req any, out any) (int, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}

	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Post("http://"+addr+path, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if out != nil {
		byts, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return resp.StatusCode, readErr
		}
		if len(byts) > 0 {
			if err := json.Unmarshal(byts, out); err != nil {
				return resp.StatusCode, err
			}
		}
	}
	return resp.StatusCode, nil
}

// leaderPositionForGroup returns the position of the current leader within the
// group's participant list, or -1 if no active leader can be identified.
//
// This position is exactly what the public metadata API exposes as LeaderIdx.
func (p *HKVCParticipant) leaderPositionForGroup(gid int) int {
	members := p.groups[gid]
	for pos, clusterIdx := range members {
		if clusterIdx == p.myIndex {
			if p.isLeader(gid) {
				return pos
			}
			continue
		}

		var resp internalLeaderStatus
		code, err := p.internalPost(
			p.info[clusterIdx].ClientAddr,
			"/_hkvc_internal/leader_status",
			internalGroupRequest{GroupID: gid},
			&resp,
		)
		if err != nil || code != http.StatusOK {
			continue
		}
		if resp.Active && resp.Leader {
			return pos
		}
	}
	return -1
}

// routeViaGroup0Leader asks the current base-group leader which group owns the
// supplied directory.
//
// If this participant is already the base-group leader, it answers locally.
func (p *HKVCParticipant) routeViaGroup0Leader(dir string) (int, bool) {
	if p.isLeader(0) {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.routeGroupLocked(dir), true
	}

	req := DirectoryRequest{Directory: dir}
	for _, clusterIdx := range p.groups[0] {
		var resp internalRouteResponse
		code, err := p.internalPost(
			p.info[clusterIdx].ClientAddr,
			"/_hkvc_internal/route",
			req,
			&resp,
		)
		if err != nil {
			continue
		}
		if code == http.StatusOK {
			return resp.GroupID, true
		}
	}
	return 0, false
}

// waitUntilDirectoryVisibleOnOwnerLeader waits briefly until the leader of the
// owning group can see a newly created directory in its local state.
//
// This is used after successful directory creation so that follow-up tests which
// immediately probe the new directory's leader through /list behave reliably.
func (p *HKVCParticipant) waitUntilDirectoryVisibleOnOwnerLeader(dir string) {
	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		gid, ok := p.routeViaGroup0Leader(dir)
		if ok {
			leaderPos := p.leaderPositionForGroup(gid)
			if leaderPos >= 0 && leaderPos < len(p.groups[gid]) {
				clusterIdx := p.groups[gid][leaderPos]

				var resp internalDirExistsResponse
				code, err := p.internalPost(
					p.info[clusterIdx].ClientAddr,
					"/_hkvc_internal/dir_exists",
					internalDirExistsRequest{Directory: dir},
					&resp,
				)
				if err == nil && code == http.StatusOK && resp.Exists {
					return
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// commitViaLeader submits a command to the leader of the specified group.
//
// If this participant is already the leader, submission happens locally.
// Otherwise, the command is forwarded to the group leader through an internal
// helper endpoint.
func (p *HKVCParticipant) commitViaLeader(gid int, cmd ClusterCommand) StoredResponse {
	if p.isLeader(gid) {
		return p.submitCommand(gid, cmd)
	}

	members := p.groups[gid]
	for _, clusterIdx := range members {
		var resp StoredResponse
		code, err := p.internalPost(
			p.info[clusterIdx].ClientAddr,
			"/_hkvc_internal/commit_group",
			internalCommitRequest{GroupID: gid, Command: cmd},
			&resp,
		)
		if err != nil {
			continue
		}
		if code == http.StatusOK {
			return resp
		}
	}

	return errorResponse(cmd.ClientID, NonLeaderError, "group leader unavailable", http.StatusForbidden)
}

// submitCommand validates client sequencing for one group, suppresses duplicate
// in-flight appends, submits the command to Raft, and waits for the apply loop
// to deliver the final response.
//
// This is the core bridge between HTTP request handling and replicated state
// machine application.
func (p *HKVCParticipant) submitCommand(gid int, cmd ClusterCommand) StoredResponse {
	if !p.isLeader(gid) {
		return errorResponse(cmd.ClientID, NonLeaderError, "not leader for requested group", http.StatusForbidden)
	}

	key := opKey(cmd.ClientID, cmd.SeqNumber)

	p.mu.Lock()
	histMap := p.clientHistory[gid]
	if hist, ok := histMap[cmd.ClientID]; ok {
		if cmd.SeqNumber < hist.LastSeq {
			resp := errorResponse(cmd.ClientID, OutOfSequenceError, "message sequence number is older than the previous request", http.StatusNotAcceptable)
			p.mu.Unlock()
			return resp
		}
		if cmd.SeqNumber == hist.LastSeq {
			resp := hist.Response
			p.mu.Unlock()
			return resp
		}
	}

	waitCh := make(chan StoredResponse, 1)
	if _, alreadyPending := p.pendingReqs[gid][key]; alreadyPending {
		// Duplicate in-flight request: attach to the waiter list instead of
		// appending another Raft log entry.
		p.pendingReqs[gid][key] = append(p.pendingReqs[gid][key], waitCh)
		p.mu.Unlock()
	} else {
		p.pendingReqs[gid][key] = []chan StoredResponse{waitCh}
		p.mu.Unlock()

		byts, err := json.Marshal(cmd)
		if err != nil {
			p.mu.Lock()
			delete(p.pendingReqs[gid], key)
			p.mu.Unlock()
			return errorResponse(cmd.ClientID, InvalidError, "failed to encode command", http.StatusBadRequest)
		}

		status, re := p.raftNodes[gid].NewCommand(byts)
		if re.Error() != "" || !status.Leader {
			p.mu.Lock()
			delete(p.pendingReqs[gid], key)
			p.mu.Unlock()
			return errorResponse(cmd.ClientID, NonLeaderError, "not leader for requested group", http.StatusForbidden)
		}
	}

	defer func() {
		p.mu.Lock()
		waiters := p.pendingReqs[gid][key]
		kept := make([]chan StoredResponse, 0, len(waiters))
		for _, ch := range waiters {
			if ch != waitCh {
				kept = append(kept, ch)
			}
		}
		if len(kept) == 0 {
			delete(p.pendingReqs[gid], key)
		} else {
			p.pendingReqs[gid][key] = kept
		}
		p.mu.Unlock()
	}()

	select {
	case resp := <-waitCh:
		return resp
	case <-time.After(5 * time.Second):
		return errorResponse(cmd.ClientID, NonLeaderError, "timed out waiting for committed command", http.StatusForbidden)
	}
}

// ensureDirDataLocked creates the key/value map for a directory path if it does
// not already exist.
//
// The caller must already hold p.mu.
func (p *HKVCParticipant) ensureDirDataLocked(path string) {
	if _, ok := p.dirData[path]; !ok {
		p.dirData[path] = make(map[string]*KeyRecord)
	}
}

// removeSubtreeDataLocked removes all ordinary key/value maps for a deleted
// directory subtree.
//
// The caller must already hold p.mu.
func (p *HKVCParticipant) removeSubtreeDataLocked(path string) {
	for dirPath := range p.dirData {
		if hasPathPrefix(dirPath, path) {
			delete(p.dirData, dirPath)
		}
	}
}

// cloneSubtreeToGroup0Locked rewrites the GroupID of a subtree to 0 before that
// subtree is discarded.
//
// This is a conservative cleanup step used when deleting directories so any
// transient stale views cannot continue to point at vanished non-zero groups.
func (p *HKVCParticipant) cloneSubtreeToGroup0Locked(node *DirNode) {
	if node == nil {
		return
	}
	node.GroupID = 0
	for _, child := range node.Subdirs {
		p.cloneSubtreeToGroup0Locked(child)
	}
}

// listDirectoryLocked constructs the successful /list response for a directory.
//
// Both immediate subdirectories and immediate keys are included in the result.
func (p *HKVCParticipant) listDirectoryLocked(dir string, clientID string) StoredResponse {
	lookup := p.lookupDirectoryLocked(dir)
	if lookup.Node == nil {
		return errorResponse(clientID, DirNotFoundError, "directory not found", http.StatusNotFound)
	}

	items := make([]string, 0)

	for name := range lookup.Node.Subdirs {
		items = append(items, name)
	}
	if kvs, ok := p.dirData[dir]; ok {
		for name := range kvs {
			items = append(items, name)
		}
	}

	sort.Strings(items)

	return StoredResponse{
		Kind:      "list",
		Status:    http.StatusOK,
		Directory: dir,
		List:      items,
		ClientID:  clientID,
	}
}

// metadataForDirectoryLocked constructs the successful /get_metadata response
// for a directory object.
//
// The key argument may be "." to refer to the directory itself or a child
// directory name to refer to one immediate subdirectory.
func (p *HKVCParticipant) metadataForDirectoryLocked(dir string, key string, clientID string) StoredResponse {
	var node *DirNode

	if key == "." {
		lookup := p.lookupDirectoryLocked(dir)
		if lookup.Node == nil {
			return errorResponse(clientID, DirNotFoundError, "directory not found", http.StatusNotFound)
		}
		node = lookup.Node
	} else {
		lookup := p.lookupDirectoryLocked(dir)
		if lookup.Node == nil {
			return errorResponse(clientID, DirNotFoundError, "directory not found", http.StatusNotFound)
		}
		child, ok := lookup.Node.Subdirs[key]
		if !ok {
			return errorResponse(clientID, KeyNotFoundError, "key not found", http.StatusNotFound)
		}
		node = child
	}

	gid := node.GroupID
	return StoredResponse{
		Kind:        "metadata",
		Status:      http.StatusOK,
		Directory:   dir,
		Key:         key,
		IsDirectory: true,
		Size:        -1,
		Version:     node.Version,
		PAddrList:   p.groupAddressList(gid),
		LeaderIdx:   p.leaderPositionForGroup(gid),
		Tags:        cloneStrings(node.Tags),
		ClientID:    clientID,
	}
}

// metadataForKeyLocked constructs the successful /get_metadata response for an
// ordinary key stored inside a directory.
func (p *HKVCParticipant) metadataForKeyLocked(dir string, key string, clientID string) StoredResponse {
	lookup := p.lookupDirectoryLocked(dir)
	if lookup.Node == nil {
		return errorResponse(clientID, DirNotFoundError, "directory not found", http.StatusNotFound)
	}

	kvs := p.dirData[dir]
	rec, ok := kvs[key]
	if !ok {
		return errorResponse(clientID, KeyNotFoundError, "key not found", http.StatusNotFound)
	}

	gid := lookup.Node.GroupID
	return StoredResponse{
		Kind:        "metadata",
		Status:      http.StatusOK,
		Directory:   dir,
		Key:         key,
		IsDirectory: false,
		Size:        len(rec.Value),
		Version:     rec.Version,
		PAddrList:   p.groupAddressList(gid),
		LeaderIdx:   p.leaderPositionForGroup(gid),
		Tags:        cloneStrings(rec.Tags),
		ClientID:    clientID,
	}
}

// applyBaseCreateLocked applies a committed directory creation command from
// group 0.
//
// Because directory topology is globally coordinated, create always executes in
// the base group even if the new directory will eventually be owned by some
// other group for ordinary key/value contents.
func (p *HKVCParticipant) applyBaseCreateLocked(cmd ClusterCommand) StoredResponse {
	lookup := p.lookupDirectoryLocked(cmd.Directory)
	if lookup.KeyInPath {
		return errorResponse(cmd.ClientID, ConflictKeyError, "directory path refers to an existing key", http.StatusConflict)
	}
	if lookup.Node == nil {
		return errorResponse(cmd.ClientID, DirNotFoundError, "directory not found", http.StatusNotFound)
	}

	if kvs, ok := p.dirData[cmd.Directory]; ok {
		if _, existsKey := kvs[cmd.Key]; existsKey {
			return errorResponse(cmd.ClientID, ConflictKeyError, "directory name conflicts with an existing key", http.StatusConflict)
		}
	}

	if _, existsDir := lookup.Node.Subdirs[cmd.Key]; existsDir {
		return StoredResponse{
			Kind:      "key_success",
			Status:    http.StatusOK,
			Directory: cmd.Directory,
			Key:       cmd.Key,
			Success:   false,
			ClientID:  cmd.ClientID,
		}
	}

	newPath := joinPath(cmd.Directory, cmd.Key)
	child := &DirNode{
		Name:     cmd.Key,
		GroupID:  p.nextAssignedGroupLocked(),
		Version:  1,
		Tags:     []string{},
		Subdirs:  make(map[string]*DirNode),
		Parent:   lookup.Node,
		FullPath: newPath,
	}
	lookup.Node.Subdirs[cmd.Key] = child
	p.ensureDirDataLocked(newPath)

	return StoredResponse{
		Kind:      "key_success",
		Status:    http.StatusCreated,
		Directory: cmd.Directory,
		Key:       cmd.Key,
		Success:   true,
		ClientID:  cmd.ClientID,
	}
}

// applyBaseDeleteDirectoryLocked applies a committed directory deletion command
// from group 0.
//
// Deleting a directory removes the full directory subtree and all ordinary
// key/value maps beneath it.
func (p *HKVCParticipant) applyBaseDeleteDirectoryLocked(cmd ClusterCommand) StoredResponse {
	lookup := p.lookupDirectoryLocked(cmd.Directory)
	if lookup.Node == nil {
		return errorResponse(cmd.ClientID, DirNotFoundError, "directory not found", http.StatusNotFound)
	}

	child, ok := lookup.Node.Subdirs[cmd.Key]
	if !ok {
		return errorResponse(cmd.ClientID, KeyNotFoundError, "key not found", http.StatusNotFound)
	}

	// Normalize deleted subtree to group 0 before removing it so future stale reads
	// do not accidentally point at vanished non-zero groups.
	p.cloneSubtreeToGroup0Locked(child)

	delete(lookup.Node.Subdirs, cmd.Key)
	p.removeSubtreeDataLocked(child.FullPath)

	return StoredResponse{
		Kind:      "key_success",
		Status:    http.StatusOK,
		Directory: cmd.Directory,
		Key:       cmd.Key,
		Success:   true,
		ClientID:  cmd.ClientID,
	}
}

// applyGroupListLocked applies a committed list-style read for a directory-owned
// group.
func (p *HKVCParticipant) applyGroupListLocked(cmd ClusterCommand) StoredResponse {
	return p.listDirectoryLocked(cmd.Directory, cmd.ClientID)
}

// applyGroupGetLocked applies a committed get for an ordinary key within one
// directory-owned group.
func (p *HKVCParticipant) applyGroupGetLocked(cmd ClusterCommand) StoredResponse {
	lookup := p.lookupDirectoryLocked(cmd.Directory)
	if lookup.Node == nil {
		return errorResponse(cmd.ClientID, DirNotFoundError, "directory not found", http.StatusNotFound)
	}

	kvs := p.dirData[cmd.Directory]
	rec, ok := kvs[cmd.Key]
	if !ok {
		return errorResponse(cmd.ClientID, KeyNotFoundError, "key not found", http.StatusNotFound)
	}

	return StoredResponse{
		Kind:      "key_value",
		Status:    http.StatusOK,
		Directory: cmd.Directory,
		Key:       cmd.Key,
		Value:     rec.Value,
		ClientID:  cmd.ClientID,
	}
}

// applyGroupSetLocked applies a committed set for an ordinary key within one
// directory-owned group.
func (p *HKVCParticipant) applyGroupSetLocked(cmd ClusterCommand) StoredResponse {
	lookup := p.lookupDirectoryLocked(cmd.Directory)
	if lookup.KeyInPath {
		return errorResponse(cmd.ClientID, ConflictKeyError, "directory path refers to an existing key", http.StatusConflict)
	}
	if lookup.Node == nil {
		return errorResponse(cmd.ClientID, DirNotFoundError, "directory not found", http.StatusNotFound)
	}

	if _, existsDir := lookup.Node.Subdirs[cmd.Key]; existsDir {
		return errorResponse(cmd.ClientID, ConflictDirError, "key conflicts with an existing directory", http.StatusConflict)
	}

	p.ensureDirDataLocked(cmd.Directory)
	kvs := p.dirData[cmd.Directory]
	rec, ok := kvs[cmd.Key]
	if !ok {
		kvs[cmd.Key] = &KeyRecord{
			Value:   cmd.Value,
			Version: 1,
			Tags:    []string{},
		}
		return StoredResponse{
			Kind:      "key_success",
			Status:    http.StatusCreated,
			Directory: cmd.Directory,
			Key:       cmd.Key,
			Success:   true,
			ClientID:  cmd.ClientID,
		}
	}

	rec.Value = cmd.Value
	rec.Version++
	return StoredResponse{
		Kind:      "key_success",
		Status:    http.StatusOK,
		Directory: cmd.Directory,
		Key:       cmd.Key,
		Success:   true,
		ClientID:  cmd.ClientID,
	}
}

// applyGroupDeleteKeyLocked applies a committed delete for an ordinary key.
func (p *HKVCParticipant) applyGroupDeleteKeyLocked(cmd ClusterCommand) StoredResponse {
	lookup := p.lookupDirectoryLocked(cmd.Directory)
	if lookup.Node == nil {
		return errorResponse(cmd.ClientID, DirNotFoundError, "directory not found", http.StatusNotFound)
	}

	kvs := p.dirData[cmd.Directory]
	if _, ok := kvs[cmd.Key]; !ok {
		return errorResponse(cmd.ClientID, KeyNotFoundError, "key not found", http.StatusNotFound)
	}

	delete(kvs, cmd.Key)

	return StoredResponse{
		Kind:      "key_success",
		Status:    http.StatusOK,
		Directory: cmd.Directory,
		Key:       cmd.Key,
		Success:   true,
		ClientID:  cmd.ClientID,
	}
}

// applyCommand is the single entry point from Raft apply messages into the HKVC
// state machine for one group.
//
// It enforces per-group client sequencing and dispatches to the specific apply
// helper based on command type.
func (p *HKVCParticipant) applyCommand(gid int, cmd ClusterCommand) StoredResponse {
	p.mu.Lock()

	if hist, ok := p.clientHistory[gid][cmd.ClientID]; ok {
		if cmd.SeqNumber < hist.LastSeq {
			p.mu.Unlock()
			return errorResponse(cmd.ClientID, OutOfSequenceError, "message sequence number is older than the previous request", http.StatusNotAcceptable)
		}
		if cmd.SeqNumber == hist.LastSeq {
			resp := hist.Response
			p.mu.Unlock()
			return resp
		}
	}

	var resp StoredResponse

	switch cmd.Endpoint {
	case "create":
		resp = p.applyBaseCreateLocked(cmd)

	case "delete":
		if cmd.AffectsDirectory {
			resp = p.applyBaseDeleteDirectoryLocked(cmd)
		} else {
			resp = p.applyGroupDeleteKeyLocked(cmd)
		}

	case "list":
		resp = p.applyGroupListLocked(cmd)

	case "get":
		resp = p.applyGroupGetLocked(cmd)

	case "set":
		resp = p.applyGroupSetLocked(cmd)

	case "get_metadata":
		if cmd.AffectsDirectory {
			resp = p.metadataForDirectoryLocked(cmd.Directory, cmd.Key, cmd.ClientID)
		} else {
			resp = p.metadataForKeyLocked(cmd.Directory, cmd.Key, cmd.ClientID)
		}

	default:
		resp = errorResponse(cmd.ClientID, InvalidError, "unknown endpoint", http.StatusBadRequest)
	}

	p.clientHistory[gid][cmd.ClientID] = clientHistory{
		LastSeq:  cmd.SeqNumber,
		Response: resp,
	}
	p.mu.Unlock()

	return resp
}

// applyLoop reads committed Raft log entries for one local group, applies them
// to the HKVC state machine, and wakes any HTTP handlers waiting on the
// corresponding response.
func (p *HKVCParticipant) applyLoop(gid int, ch chan raft.ApplyMsg) {
	for msg := range ch {
		var cmd ClusterCommand
		if err := json.Unmarshal(msg.Command, &cmd); err != nil {
			continue
		}

		resp := p.applyCommand(gid, cmd)
		key := opKey(cmd.ClientID, cmd.SeqNumber)

		p.mu.Lock()
		waiters := p.pendingReqs[gid][key]
		if len(waiters) > 0 {
			delete(p.pendingReqs[gid], key)
		}
		p.mu.Unlock()

		for _, waiter := range waiters {
			select {
			case waiter <- resp:
			default:
			}
		}
	}
}

// decodeRequest enforces the common request preconditions shared by all HTTP
// endpoints:
//
//   - method must be POST
//   - body must be readable
//   - body must decode as the expected JSON request type
//
// On failure it writes the appropriate public error response and returns false.
func (p *HKVCParticipant) decodeRequest(w http.ResponseWriter, r *http.Request, v any, clientID *string) bool {
	if r.Method != http.MethodPost {
		p.writeStoredResponse(w, errorResponse("", InvalidError, "only POST is supported", http.StatusBadRequest))
		return false
	}

	byts, err := io.ReadAll(r.Body)
	if err != nil {
		id := ""
		if clientID != nil {
			id = *clientID
		}
		p.writeStoredResponse(w, errorResponse(id, InvalidError, "unable to read request body", http.StatusBadRequest))
		return false
	}

	if err := json.Unmarshal(byts, v); err != nil {
		id := ""
		if clientID != nil {
			id = *clientID
		}
		p.writeStoredResponse(w, errorResponse(id, InvalidError, "unable to decode request body", http.StatusBadRequest))
		return false
	}
	return true
}

// routeGroup is the unlocked convenience wrapper around routeGroupLocked.
func (p *HKVCParticipant) routeGroup(dir string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.routeGroupLocked(dir)
}

// childIsDirectory reports whether key names a child directory immediately
// inside dir.
//
// The special key "." is treated as the directory itself.
func (p *HKVCParticipant) childIsDirectory(dir, key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if key == "." {
		lookup := p.lookupDirectoryLocked(dir)
		return lookup.Node != nil
	}

	lookup := p.lookupDirectoryLocked(dir)
	if lookup.Node == nil {
		return false
	}
	_, ok := lookup.Node.Subdirs[key]
	return ok
}

// handleList implements the public /list endpoint.
//
// The request must be sent to the leader of the group that owns the target
// directory. The actual list result is then produced by a committed Raft command
// in that group.
func (p *HKVCParticipant) handleList(w http.ResponseWriter, r *http.Request) {
	req := DirectoryRequest{}
	if !p.decodeRequest(w, r, &req, &req.ClientID) {
		return
	}

	dir, ok := normalizeDirectory(req.Directory)
	if !ok {
		p.writeStoredResponse(w, errorResponse(req.ClientID, InvalidError, "invalid directory", http.StatusBadRequest))
		return
	}

	groupID := p.routeGroup(dir)
	if !p.isLeader(groupID) {
		p.writeStoredResponse(w, errorResponse(req.ClientID, NonLeaderError, "not leader for requested directory", http.StatusForbidden))
		return
	}

	cmd := ClusterCommand{
		Endpoint:   "list",
		Directory:  dir,
		ClientID:   req.ClientID,
		SeqNumber:  req.SeqNumber,
		RouteGroup: groupID,
	}
	p.writeStoredResponse(w, p.commitViaLeader(groupID, cmd))
}

// handleGetMetadata implements the public /get_metadata endpoint.
//
// Directory metadata is served through group 0 because directory topology is
// globally coordinated there. Ordinary key metadata is served through the
// directory's owning group.
func (p *HKVCParticipant) handleGetMetadata(w http.ResponseWriter, r *http.Request) {
	req := KeyRequest{}
	if !p.decodeRequest(w, r, &req, &req.ClientID) {
		return
	}

	dir, ok := normalizeDirectory(req.Directory)
	if !ok || !isValidMetadataKey(req.Key) {
		p.writeStoredResponse(w, errorResponse(req.ClientID, InvalidError, "invalid directory or key", http.StatusBadRequest))
		return
	}

	affectsDirectory := req.Key == "." || p.childIsDirectory(dir, req.Key)
	groupID := p.routeGroup(dir)
	if !affectsDirectory && !p.isLeader(groupID) {
		p.writeStoredResponse(w, errorResponse(req.ClientID, NonLeaderError, "not leader for requested directory", http.StatusForbidden))
		return
	}

	cmd := ClusterCommand{
		Endpoint:         "get_metadata",
		Directory:        dir,
		Key:              req.Key,
		ClientID:         req.ClientID,
		SeqNumber:        req.SeqNumber,
		RouteGroup:       groupID,
		AffectsDirectory: affectsDirectory,
	}

	if affectsDirectory {
		p.writeStoredResponse(w, p.commitViaLeader(0, cmd))
		return
	}
	p.writeStoredResponse(w, p.commitViaLeader(groupID, cmd))
}

// handleGet implements the public /get endpoint.
//
// The request must reach the leader of the group that owns the target
// directory.
func (p *HKVCParticipant) handleGet(w http.ResponseWriter, r *http.Request) {
	req := KeyRequest{}
	if !p.decodeRequest(w, r, &req, &req.ClientID) {
		return
	}

	dir, ok := normalizeDirectory(req.Directory)
	if !ok || !isValidKey(req.Key) {
		p.writeStoredResponse(w, errorResponse(req.ClientID, InvalidError, "invalid directory or key", http.StatusBadRequest))
		return
	}

	groupID := p.routeGroup(dir)
	if !p.isLeader(groupID) {
		p.writeStoredResponse(w, errorResponse(req.ClientID, NonLeaderError, "not leader for requested directory", http.StatusForbidden))
		return
	}

	cmd := ClusterCommand{
		Endpoint:   "get",
		Directory:  dir,
		Key:        req.Key,
		ClientID:   req.ClientID,
		SeqNumber:  req.SeqNumber,
		RouteGroup: groupID,
	}
	p.writeStoredResponse(w, p.commitViaLeader(groupID, cmd))
}

// handleSet implements the public /set endpoint.
//
// The target directory must already exist. The operation is committed through
// the group that owns that directory's ordinary keys.
func (p *HKVCParticipant) handleSet(w http.ResponseWriter, r *http.Request) {
	req := KeyValueMessage{}
	if !p.decodeRequest(w, r, &req, &req.ClientID) {
		return
	}

	dir, ok := normalizeDirectory(req.Directory)
	if !ok || !isValidKey(req.Key) {
		p.writeStoredResponse(w, errorResponse(req.ClientID, InvalidError, "invalid directory or key", http.StatusBadRequest))
		return
	}

	groupID := p.routeGroup(dir)
	if !p.isLeader(groupID) {
		p.writeStoredResponse(w, errorResponse(req.ClientID, NonLeaderError, "not leader for requested directory", http.StatusForbidden))
		return
	}

	cmd := ClusterCommand{
		Endpoint:   "set",
		Directory:  dir,
		Key:        req.Key,
		Value:      req.Value,
		ClientID:   req.ClientID,
		SeqNumber:  req.SeqNumber,
		RouteGroup: groupID,
	}
	p.writeStoredResponse(w, p.commitViaLeader(groupID, cmd))
}

// handleCreate implements the public /create endpoint.
//
// A client must send create to the leader of the parent directory's owning
// group. The actual directory topology change is committed through group 0. On
// success, the handler briefly waits until the new directory is visible on the
// leader of its assigned owning group so immediate follow-up discovery calls are
// stable.
func (p *HKVCParticipant) handleCreate(w http.ResponseWriter, r *http.Request) {
	req := KeyRequest{}
	if !p.decodeRequest(w, r, &req, &req.ClientID) {
		return
	}

	dir, ok := normalizeDirectory(req.Directory)
	if !ok || !isValidKey(req.Key) {
		p.writeStoredResponse(w, errorResponse(req.ClientID, InvalidError, "invalid directory or key", http.StatusBadRequest))
		return
	}

	parentGroup := p.routeGroup(dir)
	if !p.isLeader(parentGroup) {
		p.writeStoredResponse(w, errorResponse(req.ClientID, NonLeaderError, "not leader for requested directory", http.StatusForbidden))
		return
	}

	cmd := ClusterCommand{
		Endpoint:         "create",
		Directory:        dir,
		Key:              req.Key,
		ClientID:         req.ClientID,
		SeqNumber:        req.SeqNumber,
		RouteGroup:       parentGroup,
		AffectsDirectory: true,
	}

	resp := p.commitViaLeader(0, cmd)

	if resp.Kind == "key_success" && resp.Status == http.StatusCreated && resp.Success {
		newDir := joinPath(dir, req.Key)
		p.waitUntilDirectoryVisibleOnOwnerLeader(newDir)
	}

	p.writeStoredResponse(w, resp)
}

// handleDelete implements the public /delete endpoint.
//
// If the target names a child directory, the delete is a topology change and is
// committed through group 0. If it names an ordinary key, the delete is
// committed through the parent directory's owning group.
func (p *HKVCParticipant) handleDelete(w http.ResponseWriter, r *http.Request) {
	req := KeyRequest{}
	if !p.decodeRequest(w, r, &req, &req.ClientID) {
		return
	}

	dir, ok := normalizeDirectory(req.Directory)
	if !ok || !isValidKey(req.Key) {
		p.writeStoredResponse(w, errorResponse(req.ClientID, InvalidError, "invalid directory or key", http.StatusBadRequest))
		return
	}

	parentGroup := p.routeGroup(dir)
	if !p.isLeader(parentGroup) {
		p.writeStoredResponse(w, errorResponse(req.ClientID, NonLeaderError, "not leader for requested directory", http.StatusForbidden))
		return
	}

	isDirDelete := p.childIsDirectory(dir, req.Key)
	cmd := ClusterCommand{
		Endpoint:         "delete",
		Directory:        dir,
		Key:              req.Key,
		ClientID:         req.ClientID,
		SeqNumber:        req.SeqNumber,
		RouteGroup:       parentGroup,
		AffectsDirectory: isDirDelete,
	}

	if isDirDelete {
		p.writeStoredResponse(w, p.commitViaLeader(0, cmd))
		return
	}
	p.writeStoredResponse(w, p.commitViaLeader(parentGroup, cmd))
}

// handleInternalRoute returns the group that currently owns the supplied
// directory path.
//
// Only the base-group leader may answer this request successfully.
func (p *HKVCParticipant) handleInternalRoute(w http.ResponseWriter, r *http.Request) {
	req := DirectoryRequest{}
	if !p.decodeRequest(w, r, &req, &req.ClientID) {
		return
	}
	if !p.isLeader(0) {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	dir, ok := normalizeDirectory(req.Directory)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	p.mu.Lock()
	groupID := p.routeGroupLocked(dir)
	p.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(internalRouteResponse{GroupID: groupID})
}

// handleInternalCommitGroup asks the receiver to submit the supplied command to
// the specified group if it is that group's current leader.
func (p *HKVCParticipant) handleInternalCommitGroup(w http.ResponseWriter, r *http.Request) {
	req := internalCommitRequest{}
	if !p.decodeRequest(w, r, &req, &req.Command.ClientID) {
		return
	}
	if !p.isLeader(req.GroupID) {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(p.submitCommand(req.GroupID, req.Command))
}

// handleInternalLeaderStatus returns whether the receiver is the active leader
// of the requested group.
func (p *HKVCParticipant) handleInternalLeaderStatus(w http.ResponseWriter, r *http.Request) {
	req := internalGroupRequest{}
	if !p.decodeRequest(w, r, &req, nil) {
		return
	}

	node, ok := p.raftNodes[req.GroupID]
	if !ok {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(internalLeaderStatus{})
		return
	}

	status, _ := node.GetStatus()
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(internalLeaderStatus{
		Leader: status.Leader,
		Active: status.Active,
	})
}

// handleInternalDirExists reports whether the supplied directory path is
// currently present in the receiver's local hierarchy view.
func (p *HKVCParticipant) handleInternalDirExists(w http.ResponseWriter, r *http.Request) {
	req := internalDirExistsRequest{}
	if !p.decodeRequest(w, r, &req, nil) {
		return
	}

	dir, ok := normalizeDirectory(req.Directory)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	p.mu.Lock()
	lookup := p.lookupDirectoryLocked(dir)
	exists := lookup.Node != nil
	p.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(internalDirExistsResponse{
		Exists: exists,
	})
}

// Activate starts the participant's public HTTP interface and activates all
// local Raft peers.
//
// The control stub is already running before this method is called; Activate
// transitions the participant into the fully usable state expected by the
// public tests.
func (p *HKVCParticipant) Activate() remote.RemoteError {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.active {
		return remote.RemoteError{}
	}

	for _, rn := range p.raftNodes {
		if re := rn.Activate(); re.Error() != "" {
			return re
		}
	}

	p.httpServer = &http.Server{
		Addr:    p.info[p.myIndex].ClientAddr,
		Handler: p.httpHandler,
	}
	ln, err := net.Listen("tcp", p.httpServer.Addr)
	if err != nil {
		return remote.RemoteError{Err: err.Error()}
	}
	p.listener = ln
	p.active = true
	go p.httpServer.Serve(ln)

	return remote.RemoteError{}
}

// Deactivate stops the public HTTP interface and deactivates all local Raft
// peers while preserving all in-memory state.
func (p *HKVCParticipant) Deactivate() remote.RemoteError {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.active {
		return remote.RemoteError{}
	}

	p.active = false
	if p.httpServer != nil {
		_ = p.httpServer.Close()
	}
	p.listener = nil

	for _, rn := range p.raftNodes {
		_ = rn.Deactivate()
	}

	return remote.RemoteError{}
}

// Terminate permanently stops the participant, including its HTTP interface,
// local Raft peers, and test control stub.
func (p *HKVCParticipant) Terminate() remote.RemoteError {
	p.mu.Lock()
	if p.httpServer != nil {
		_ = p.httpServer.Close()
	}
	p.listener = nil
	p.active = false
	p.mu.Unlock()

	for _, rn := range p.raftNodes {
		_ = rn.Terminate()
	}
	if p.controlStub != nil && p.controlStub.IsRunning() {
		_ = p.controlStub.Stop()
	}
	return remote.RemoteError{}
}

// GetStatus returns a summary of the participant's externally visible control
// state for the test harness.
func (p *HKVCParticipant) GetStatus() (HKVCStatusReport, remote.RemoteError) {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()

	sr := HKVCStatusReport{
		Active:      active,
		GroupLeader: make(map[int]bool),
		GroupCommit: make(map[int]int),
	}

	for gid, rn := range p.raftNodes {
		rs, _ := rn.GetStatus()
		sr.GroupLeader[gid] = rs.Leader && rs.Active
		if rs.Leader && rs.Active {
			sr.GroupCommit[gid] = rs.Index
		}
	}

	return sr, remote.RemoteError{}
}

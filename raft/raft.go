package raft

// Package raft contains a simplified implementation of the Raft consensus
// protocol for the lab test environment.
//
// This implementation is designed to work with the custom remote RPC library
// from the previous lab and with the provided Controller used by the tests.
// The code focuses on the subset of Raft required by the assignment:
//
//   - leader election
//   - heartbeats
//   - log replication
//   - commitment of replicated entries
//   - failure, recovery, and partition simulation through activation control
//
// The implementation intentionally does not include persistence, snapshotting,
// log compaction, cluster membership changes, or application of committed
// commands to a real state machine, because those are outside the required
// scope of this lab.
//
// Concurrency notes:
//
// Raft peers receive concurrent remote calls from both the Controller and
// other peers, so the node uses a mutex to protect all shared state. Any
// background activity that reads or modifies Raft state must either hold the
// mutex directly or work only with copies of previously captured values.
//
// Communication notes:
//
// Raft peers communicate only through the two RPCs in RaftInterface, and the
// Controller interacts with peers only through the methods in
// ControlInterface. No other inter-peer coordination mechanism is used.

import (
	"encoding/gob"
	"math/rand"
	"remote"
	"sync"
	"time"
)

func init() {
	// Register all values that may cross the custom RPC boundary through gob
	// serialization. This includes status messages, Raft RPC argument/reply
	// structs, and the RemoteError type returned by the remote package.
	gob.Register(StatusReport{})
	gob.Register(RequestVoteArgs{})
	gob.Register(RequestVoteReply{})
	gob.Register(AppendEntriesArgs{})
	gob.Register(AppendEntriesReply{})
	gob.Register(remote.RemoteError{})
}

// RaftSetupInfo stores the static configuration for a single peer in the
// cluster.
//
// Each peer receives the complete slice of setup information for all peers
// when it is created. The Id uniquely identifies the peer, Addr is the network
// address used for Raft-to-Raft RPCs, and Caddr is the address used by the
// Controller to manage and inspect the peer.
type RaftSetupInfo struct {
	Id    int    // Unique identifier assigned to the peer by the Controller.
	Addr  string // Address of the Raft RPC endpoint used by other peers.
}

// StatusReport is returned to the Controller as a compact summary of a peer's
// current state.
//
// The report includes the last log index currently stored, the peer's current
// term, whether it presently believes itself to be leader, whether it is
// active in the simulated network, and the cumulative RPC call count reported
// by the underlying stubs.
type StatusReport struct {
	Index     int  // Index of the last log entry currently stored by the peer.
	Term      int  // Current term known to the peer.
	Leader    bool // True when the peer currently believes it is the leader.
	Active    bool // True when the Raft RPC stub is active and participating.
	CallCount int  // Total number of remote calls handled by this peer.
}

// ApplyMsg represents a committed command to be applied to the state machine.
type ApplyMsg struct {
	Command []byte // The client command payload.
	Index   int    // The log index at which this command was committed.
}

// RaftInterface defines the two RPCs used internally by the Raft protocol.
//
// This is the only remote interface used for peer-to-peer Raft communication.
// RequestVote is used during elections, and AppendEntries is used for both
// heartbeats and log replication.
type RaftInterface struct {
	// RequestVote asks a peer to vote for a candidate in a given term.
	RequestVote func(RequestVoteArgs) (RequestVoteReply, remote.RemoteError)
	// AppendEntries sends heartbeats and log entries from the current leader.
	AppendEntries func(AppendEntriesArgs) (AppendEntriesReply, remote.RemoteError)
}

// LogEntry represents one entry in the replicated Raft log.
//
// In this lab, each entry contains only the term in which it was created and
// the opaque command bytes supplied by the Controller.
type LogEntry struct {
	Term    int    // Leader term in which this log entry was appended.
	Command []byte // Opaque command payload to be replicated.
}

// RequestVoteArgs contains the candidate state sent in a RequestVote RPC.
//
// The last log index and last log term are used by receivers to enforce the
// Raft election restriction that a peer should not vote for a candidate whose
// log is less up to date than its own.
type RequestVoteArgs struct {
	Term         int // Candidate's current term.
	CandidateId  int // Candidate requesting the vote.
	LastLogIndex int // Index of candidate's last log entry.
	LastLogTerm  int // Term of candidate's last log entry.
}

// RequestVoteReply is returned by a peer after evaluating a vote request.
//
// The receiver includes its current term so that outdated candidates can step
// down when they learn of a newer term.
type RequestVoteReply struct {
	Term        int  // Receiver's current term.
	VoteGranted bool // True when the vote is granted.
}

// AppendEntriesArgs contains the leader state sent in an AppendEntries RPC.
//
// This single RPC is used for both empty heartbeats and actual log
// replication. PrevLogIndex and PrevLogTerm identify the entry that must
// already exist on the follower before the new entries can be appended.
type AppendEntriesArgs struct {
	Term         int        // Leader's current term.
	LeaderId     int        // Identifier of the current leader.
	PrevLogIndex int        // Index immediately preceding Entries.
	PrevLogTerm  int        // Term of the entry at PrevLogIndex.
	Entries      []LogEntry // Entries to append; empty for heartbeat-only RPCs.
	LeaderCommit int        // Leader's current commit index.
}

// AppendEntriesReply reports whether the follower accepted the leader's
// AppendEntries request.
//
// As with RequestVoteReply, the receiver includes its current term so an
// outdated leader can step down when it learns that a newer term exists.
type AppendEntriesReply struct {
	Term    int  // Receiver's current term.
	Success bool // True when the follower accepted the append request.
}

// PeerState represents the peer's current role in the Raft protocol.
type PeerState int

const (
	Follower  PeerState = iota // Passive state; responds to leaders and candidates.
	Candidate                  // Election state; solicits votes to become leader.
	Leader                     // Active state; sends heartbeats and replicates entries.
)

// RaftNode stores all state and communication components for one Raft peer.
//
// The fields are grouped to mirror the usual Raft presentation:
//
//   - persistent-style Raft state: currentTerm, votedFor, log
//   - volatile state on all servers: commitIndex, lastApplied, role/activity
//   - volatile leader state: nextIndex, matchIndex
//   - RPC stubs and configuration data
//   - coordination channels for background activity
//
// The mutex protects all fields that may be accessed concurrently.
type RaftNode struct {
	mu sync.Mutex // Guards access to all shared RaftNode state.

	// State maintained on every peer across role changes.
	//
	// In the full Raft protocol these values are persistent. In this lab they
	// are kept only in memory because persistence is intentionally omitted.
	currentTerm int        // Latest term observed by this peer.
	votedFor    int        // Candidate voted for in currentTerm, or -1 if none.
	log         []LogEntry // Log entries; index 0 is a dummy sentinel entry.

	// Volatile state maintained on every peer.
	commitIndex int       // Highest log index known to be committed.
	lastApplied int       // Highest log index applied to a state machine.
	state       PeerState // Current Raft role of the peer.
	active      bool      // True when the Raft RPC endpoint is enabled.
	callCount   int       // Stored call count field; status is derived from stubs.

	// Leader-only volatile state, reinitialized on election victory.
	nextIndex  []int // For each follower, next log index to send.
	matchIndex []int // For each follower, highest replicated log index known.

	// Cluster metadata and RPC endpoints.
	info        []RaftSetupInfo  // Setup information for all peers in the cluster.
	myIndex     int              // This peer's index in info.
	peers       []*RaftInterface // Caller stubs for sending RPCs to other peers.
	raftStub    remote.Callee    // Callee stub serving Raft peer RPCs.

	// Channels used by the local background loop.
	electionReset chan struct{} // Non-blocking signal to reset election timeout.
	terminateCh   chan struct{} // Closed to request termination of background loop.
	applyCond     *sync.Cond    // Condition variable to wake up the apply loop.
}

// NewRaftPeer creates and starts a new Raft peer instance.
//
// The Controller calls this function in its own goroutine. The peer receives
// the full cluster configuration, identifies itself by the supplied index,
// creates caller stubs for all other peers, creates callee stubs for both the
// Controller interface and the Raft interface, starts the Controller-facing
// stub immediately, and launches the background run loop.
//
// The peer starts in the follower role and in an inactive state. Its Raft RPC
// endpoint is activated later by the Controller through Activate.
func NewRaftPeer(peerInfo []RaftSetupInfo, index int, applyCh chan ApplyMsg) *RaftNode {
	node := &RaftNode{
		currentTerm:   0,
		votedFor:      -1,
		log:           make([]LogEntry, 1), // Dummy entry keeps the log effectively 1-indexed.
		commitIndex:   0,
		lastApplied:   0,
		state:         Follower,
		active:        false, // Peers are created inactive and later activated by the Controller.
		info:          peerInfo,
		myIndex:       index,
		peers:         make([]*RaftInterface, len(peerInfo)),
		electionReset: make(chan struct{}, 1),
		terminateCh:   make(chan struct{}),
		nextIndex:     make([]int, len(peerInfo)),
		matchIndex:    make([]int, len(peerInfo)),
	}
	node.applyCond = sync.NewCond(&node.mu)
	// Create outbound caller stubs for every other peer in the cluster.
	// No caller stub is needed for the local peer itself.
	for i, pInfo := range peerInfo {
		if i != index {
			node.peers[i] = &RaftInterface{}
			err := remote.CallerStubCreator(node.peers[i], pInfo.Addr, false, false)
			if err != nil {
				panic(err)
			}
		}
	}

	var err error

	// Hold the mutex while wiring up the callee stubs so that the Controller
	// cannot race with partially initialized peer state.
	node.mu.Lock()
	// Create the Raft-facing callee stub. This stub is started and stopped by
	// Activate and Deactivate to simulate connectivity changes.
	node.raftStub, err = remote.NewCalleeStub(&RaftInterface{}, node, peerInfo[index].Addr, false, false)
	if err != nil {
		panic(err)
	}
	node.mu.Unlock()

	// Launch the peer's background timer loop and apply loop.
	go node.runLoop()
	go node.applyLoop(applyCh)
	
	return node
}

func (rn *RaftNode) applyLoop(applyCh chan ApplyMsg) {
	for {
		rn.mu.Lock()
		for rn.lastApplied >= rn.commitIndex {
			rn.applyCond.Wait()
			select {
			case <-rn.terminateCh:
				rn.mu.Unlock()
				return
			default:
			}
		}

		rn.lastApplied++
		msg := ApplyMsg{
			Command: rn.log[rn.lastApplied].Command,
			Index:   rn.lastApplied,
		}
		rn.mu.Unlock()

		applyCh <- msg
	}
}

// resetElectionTimer requests that the background run loop restart the current
// election timeout.
//
// The send is intentionally non-blocking. Only the presence of at least one
// pending reset matters, so repeated resets are coalesced by the buffered
// channel.
func (rn *RaftNode) resetElectionTimer() {
	select {
	case rn.electionReset <- struct{}{}:
	default:
		// A reset signal is already pending, so no additional send is needed.
	}
}

// getElectionTimeout returns a randomized election timeout duration.
//
// Raft requires randomized election timeouts so that peers do not repeatedly
// become candidates at exactly the same moment and produce endless split
// votes. The chosen range is tuned for the lab environment rather than copied
// directly from a production system.
func (rn *RaftNode) getElectionTimeout() time.Duration {
	// Randomized timeout in the range [300ms, 500ms).
	return time.Duration(300+rand.Intn(200)) * time.Millisecond
}

// runLoop is the peer's long-running background goroutine.
//
// The loop maintains a single election timer. The timer is reset whenever the
// peer receives evidence of a current leader or otherwise needs to postpone an
// election. When the timer expires, an active follower or candidate starts a
// new election. The loop exits only when the peer is terminated.
func (rn *RaftNode) runLoop() {
	electionTimeout := rn.getElectionTimeout()
	electionTimer := time.NewTimer(electionTimeout)

	for {
		select {
		case <-rn.terminateCh:
			// Termination permanently ends background activity for this peer.
			return
		case <-rn.electionReset:
			// Restart the timeout after hearing from a leader, granting a vote,
			// or otherwise receiving a valid reset signal.
			if !electionTimer.Stop() {
				select {
				case <-electionTimer.C:
				default:
				}
			}
			electionTimeout = rn.getElectionTimeout()
			electionTimer.Reset(electionTimeout)
		case <-electionTimer.C:
			// On timeout, an active follower or candidate starts an election.
			// Leaders ignore the timeout because they drive the cluster through
			// heartbeats instead.
			rn.mu.Lock()
			if rn.active {
				if rn.state == Follower || rn.state == Candidate {
					go rn.startElection()
				}
			}
			// Always arm a fresh timer for the next cycle.
			electionTimeout = rn.getElectionTimeout()
			electionTimer.Reset(electionTimeout)
			rn.mu.Unlock()
		}
	}
}

// startElection transitions the peer into candidate state and solicits votes
// from the rest of the cluster.
//
// The peer increments its term, votes for itself, snapshots the last-log
// metadata needed for RequestVote, and then sends concurrent vote requests to
// all other peers. If it receives a majority of votes while still in the same
// election term, it becomes leader, initializes leader replication state, and
// immediately begins sending heartbeats.
func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.myIndex
	currentTerm := rn.currentTerm

	// Capture this candidate's log freshness so receivers can evaluate the
	// election restriction.
	lastLogIndex := len(rn.log) - 1
	lastLogTerm := rn.log[lastLogIndex].Term
	rn.mu.Unlock()

	// A strict majority is required to win the election.
	votesNeeded := (len(rn.info) / 2) + 1
	votesReceived := 1 // Candidate votes for itself.

	if votesReceived >= votesNeeded {
		rn.mu.Lock()
		if rn.state == Candidate && rn.currentTerm == currentTerm && rn.active {
			rn.state = Leader
			for i := range rn.nextIndex {
				rn.nextIndex[i] = len(rn.log)
				rn.matchIndex[i] = 0
			}
			go rn.sendHeartbeats()
		}
		rn.mu.Unlock()
		return
	}

	var voteMu sync.Mutex

	// Send RequestVote RPCs concurrently to all other peers.
	for i := range rn.info {
		if i == rn.myIndex {
			continue
		}

		go func(peerIndex int) {
			rn.mu.Lock()
			// Abort if this peer is no longer participating in the same election.
			if !rn.active || rn.state != Candidate || rn.currentTerm != currentTerm {
				rn.mu.Unlock()
				return
			}
			peerStub := rn.peers[peerIndex]
			rn.mu.Unlock()

			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  rn.myIndex,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			reply, err := peerStub.RequestVote(args)
			if err.Error() != "" {
				// Failed RPCs are treated as unavailable peers.
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			// Re-check leadership/election validity after the RPC returns,
			// because state may have changed while the call was in flight.
			if !rn.active || rn.state != Candidate || rn.currentTerm != currentTerm {
				return
			}

			// Discovering a higher term forces an immediate step-down.
			if reply.Term > rn.currentTerm {
				rn.state = Follower
				rn.currentTerm = reply.Term
				rn.votedFor = -1
				rn.resetElectionTimer()
				return
			}

			if reply.VoteGranted {
				voteMu.Lock()
				votesReceived++
				if votesReceived >= votesNeeded && rn.state == Candidate {
					// The candidate wins the election and becomes leader for
					// the current term. Leader replication state is initialized
					// so followers can be brought up to date.
					rn.state = Leader
					for i := range rn.nextIndex {
						rn.nextIndex[i] = len(rn.log)
						rn.matchIndex[i] = 0
					}
					// Send immediate heartbeats so followers learn the new
					// leader without waiting for the periodic interval.
					go rn.sendHeartbeats()
				}
				voteMu.Unlock()
			}
		}(i)
	}
}

// bcastAppendEntries sends AppendEntries RPCs to all followers.
//
// The same routine handles both ordinary heartbeats and actual log
// replication. For each follower, the leader computes the follower-specific
// PrevLogIndex, PrevLogTerm, and suffix of log entries starting at that
// follower's nextIndex.
//
// On success, the leader updates nextIndex and matchIndex for that follower
// and then checks whether any entries from the current term can now be marked
// committed. On failure caused by a log mismatch, the leader decrements
// nextIndex for that follower and retries through a future append attempt.
func (rn *RaftNode) bcastAppendEntries() {
	rn.mu.Lock()
	if rn.state != Leader || !rn.active {
		rn.mu.Unlock()
		return
	}
	currentTerm := rn.currentTerm
	rn.mu.Unlock()

	for i := range rn.info {
		if i == rn.myIndex {
			continue
		}

		go func(peerIndex int) {
			rn.mu.Lock()
			// Abort if leadership changed while scheduling this RPC.
			if rn.state != Leader || !rn.active || rn.currentTerm != currentTerm {
				rn.mu.Unlock()
				return
			}

			// prevLogIndex identifies the last log position the follower is
			// expected to already have before appending any new entries.
			prevLogIndex := rn.nextIndex[peerIndex] - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 && prevLogIndex < len(rn.log) {
				prevLogTerm = rn.log[prevLogIndex].Term
			}

			// entries is the suffix of the leader's log beginning at the
			// follower's next expected index. It may be empty for a heartbeat.
			entries := []LogEntry{}
			if rn.nextIndex[peerIndex] < len(rn.log) {
				entries = append(entries, rn.log[rn.nextIndex[peerIndex]:]...)
			}

			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rn.myIndex,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rn.commitIndex,
			}

			peerStub := rn.peers[peerIndex]
			rn.mu.Unlock()

			reply, err := peerStub.AppendEntries(args)
			if err.Error() != "" {
				// Unreachable peers simply fail to advance.
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			// Discard stale results if leadership or term changed while the
			// RPC was in flight.
			if !rn.active || rn.state != Leader || rn.currentTerm != currentTerm {
				return
			}

			// A higher term from any follower invalidates this leadership.
			if reply.Term > rn.currentTerm {
				rn.state = Follower
				rn.currentTerm = reply.Term
				rn.votedFor = -1
				rn.resetElectionTimer()
				return
			}

			if reply.Success {
				// Successful replication advances the follower's known progress.
				rn.nextIndex[peerIndex] = args.PrevLogIndex + len(args.Entries) + 1
				rn.matchIndex[peerIndex] = args.PrevLogIndex + len(args.Entries)

				// Check whether any new index can be considered committed.
				// By Raft's rule, the leader commits only entries from its
				// current term once they are stored on a majority.
				commitAdv := false
				for n := len(rn.log) - 1; n > rn.commitIndex; n-- {
					if rn.log[n].Term == rn.currentTerm {
						count := 1 // Count the leader itself.
						for p := range rn.info {
							if p != rn.myIndex && rn.matchIndex[p] >= n {
								count++
							}
						}
						if count >= (len(rn.info)/2)+1 {
							rn.commitIndex = n
							rn.applyCond.Broadcast() // Wake up the apply loop
							commitAdv = true
							break
						}
					}
				}
				// If commitIndex advanced, send fresh AppendEntries so followers
				// learn the new commit index promptly.
				if commitAdv {
					go rn.bcastAppendEntries()
				}
			} else {
				// The follower rejected the append because its log does not
				// match at PrevLogIndex/PrevLogTerm. Back up nextIndex and try
				// again on a subsequent append.
				rn.nextIndex[peerIndex]--
				if rn.nextIndex[peerIndex] < 1 {
					rn.nextIndex[peerIndex] = 1
				}
				go rn.bcastAppendEntries()
			}
		}(i)
	}
}

// sendHeartbeats drives the leader's periodic heartbeat cycle.
//
// The leader first sends an immediate AppendEntries round, then schedules the
// next heartbeat using time.AfterFunc. The function exits quietly if the peer
// is no longer an active leader when the next cycle would begin.
func (rn *RaftNode) sendHeartbeats() {
	rn.bcastAppendEntries()

	rn.mu.Lock()
	if rn.state != Leader || !rn.active {
		rn.mu.Unlock()
		return
	}
	rn.mu.Unlock()

	// A heartbeat interval of 150ms keeps followers informed of leadership
	// while remaining practical for the lab environment.
	time.AfterFunc(150*time.Millisecond, func() {
		rn.sendHeartbeats()
	})
}

// Activate enables the peer's Raft-facing callee stub and allows the peer to
// participate in the protocol again.
//
// This simulates a peer joining or rejoining the network. Local Raft state is
// preserved across deactivation and reactivation. Once activated, the peer can
// again receive RequestVote and AppendEntries RPCs from other peers.
func (rn *RaftNode) Activate() remote.RemoteError {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if !rn.active {
		rn.active = true
		if !rn.raftStub.IsRunning() {
			err := rn.raftStub.Start()
			if err != nil {
				return remote.RemoteError{Err: err.Error()}
			}
		}
		// Reactivated peers should not immediately time out based on stale
		// timer state, so the election timer is reset.
		rn.resetElectionTimer()
	}
	return remote.RemoteError{}
}

// Deactivate disables the peer's Raft-facing callee stub while preserving all
// local state.
//
// This simulates a failure or disconnection from the Raft network. The
// Controller-facing stub remains active so the test harness can still inspect
// and later reactivate the peer. A peer that was leader when deactivated keeps
// that belief in local state until later Raft activity causes a role change.
func (rn *RaftNode) Deactivate() remote.RemoteError {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.active {
		rn.active = false
		if rn.raftStub.IsRunning() {
			rn.raftStub.Stop()
		}
	}
	return remote.RemoteError{}
}

// Terminate permanently shuts down the peer and both of its callee stubs.
//
// The Controller uses this during test cleanup. Termination ends the
// background run loop and stops the Raft interface.
func (rn *RaftNode) Terminate() remote.RemoteError {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	close(rn.terminateCh)
	rn.applyCond.Broadcast() // Wake up applyLoop to terminate
	if rn.raftStub != nil && rn.raftStub.IsRunning() {
		rn.raftStub.Stop()
	}
	return remote.RemoteError{}
}

// GetStatus returns a snapshot of the peer's current visible state.
//
// The Controller uses this to validate term progression, role, liveness, log
// growth, and the total number of handled RPCs.
func (rn *RaftNode) GetStatus() (StatusReport, remote.RemoteError) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	count := 0
	if rn.raftStub != nil && rn.raftStub.IsRunning() {
		count += rn.raftStub.GetCallCount()
	}

	return StatusReport{
		Index:     len(rn.log) - 1,
		Term:      rn.currentTerm,
		Leader:    rn.state == Leader,
		Active:    rn.active,
		CallCount: count,
	}, remote.RemoteError{}
}

// GetCommittedCmd returns the command stored at the given index if and only if
// that index is currently committed on this peer.
//
// If the index is out of range, refers to the dummy entry, or has not yet been
// committed, the method returns nil as the command value.
func (rn *RaftNode) GetCommittedCmd(index int) ([]byte, remote.RemoteError) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if index <= rn.commitIndex && index > 0 && index < len(rn.log) {
		return rn.log[index].Command, remote.RemoteError{}
	}
	return nil, remote.RemoteError{}
}

// NewCommand submits a new client command to this peer.
//
// Only an active leader accepts a new command. When accepted, the leader
// appends the command to its local log immediately and then triggers an
// AppendEntries round so replication can begin. The returned StatusReport
// reflects the peer's state after receiving the command.
func (rn *RaftNode) NewCommand(cmd []byte) (StatusReport, remote.RemoteError) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if !rn.active || rn.state != Leader {
		return StatusReport{}, remote.RemoteError{Err: "Not active or not leader"}
	}

	// Leaders append the client command to their own log before replication.
	rn.log = append(rn.log, LogEntry{Term: rn.currentTerm, Command: cmd})

	if len(rn.info) == 1 {
		rn.commitIndex = len(rn.log) - 1
		rn.applyCond.Broadcast() // Wake up the apply loop
	} else {
		// Trigger replication immediately rather than waiting for the next
		// scheduled heartbeat cycle.
		go rn.bcastAppendEntries()
	}

	count := 0
	if rn.raftStub != nil && rn.raftStub.IsRunning() {
		count += rn.raftStub.GetCallCount()
	}

	return StatusReport{
		Index:     len(rn.log) - 1,
		Term:      rn.currentTerm,
		Leader:    true,
		Active:    true,
		CallCount: count,
	}, remote.RemoteError{}
}

//// method implementations for the RaftInterface

// RequestVote handles an incoming RequestVote RPC from a candidate.
//
// The method follows the standard Raft voting rules:
//
//   - reject candidates from older terms
//   - update local term and step down on newer terms
//   - grant a vote only if the peer has not already voted for someone else in
//     the term and the candidate's log is at least as up to date as the
//     receiver's log
//
// Granting a vote resets the election timer because it indicates recent valid
// protocol activity.
func (rn *RaftNode) RequestVote(args RequestVoteArgs) (RequestVoteReply, remote.RemoteError) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if !rn.active {
		return RequestVoteReply{Term: rn.currentTerm, VoteGranted: false}, remote.RemoteError{}
	}

	// Reject vote requests from older terms immediately.
	if args.Term < rn.currentTerm {
		return RequestVoteReply{Term: rn.currentTerm, VoteGranted: false}, remote.RemoteError{}
	}

	// Seeing a newer term causes this peer to step down and clear its prior
	// vote for the older term.
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.state = Follower
		rn.votedFor = -1
	}

	// Compare the candidate's log with the receiver's log to enforce Raft's
	// election restriction. A log is considered at least as up to date if its
	// last term is greater, or if the last terms are equal and its last index
	// is at least as large.
	lastLogIndex := len(rn.log) - 1
	lastLogTerm := rn.log[lastLogIndex].Term

	logIsUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		logIsUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		logIsUpToDate = true
	}

	if (rn.votedFor == -1 || rn.votedFor == args.CandidateId) && logIsUpToDate {
		rn.votedFor = args.CandidateId
		rn.resetElectionTimer()
		return RequestVoteReply{Term: rn.currentTerm, VoteGranted: true}, remote.RemoteError{}
	}

	return RequestVoteReply{Term: rn.currentTerm, VoteGranted: false}, remote.RemoteError{}
}

// AppendEntries handles an incoming AppendEntries RPC from the current leader.
//
// The method serves two purposes:
//
//   - heartbeat processing when Entries is empty
//   - log replication when Entries contains one or more entries
//
// The method follows the standard Raft follower rules:
//
//   - reject leaders from older terms
//   - update term and step down when a valid newer leader is observed
//   - verify the log matches at PrevLogIndex/PrevLogTerm
//   - delete conflicting entries and append new ones
//   - advance commitIndex according to the leader's commit information
//
// Receiving a valid AppendEntries resets the election timer because it
// confirms the existence of a current leader.
func (rn *RaftNode) AppendEntries(args AppendEntriesArgs) (AppendEntriesReply, remote.RemoteError) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if !rn.active {
		return AppendEntriesReply{Term: rn.currentTerm, Success: false}, remote.RemoteError{}
	}

	// Reject messages from an older leader term.
	if args.Term < rn.currentTerm {
		return AppendEntriesReply{Term: rn.currentTerm, Success: false}, remote.RemoteError{}
	}

	// A valid AppendEntries from a newer term updates term state and causes
	// this peer to behave as a follower.
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.votedFor = -1
	}
	rn.state = Follower
	rn.resetElectionTimer()

	// The follower must already contain the entry immediately preceding the
	// new entries, and its term must match exactly.
	if args.PrevLogIndex >= len(rn.log) {
		return AppendEntriesReply{Term: rn.currentTerm, Success: false}, remote.RemoteError{}
	}
	if args.PrevLogIndex >= 0 && rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return AppendEntriesReply{Term: rn.currentTerm, Success: false}, remote.RemoteError{}
	}

	// Append new entries, truncating any conflicting suffix first. If an entry
	// already exists at the target index with a different term, the follower
	// deletes that entry and everything after it before appending the leader's
	// version.
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index < len(rn.log) {
			if rn.log[index].Term != entry.Term {
				rn.log = rn.log[:index]
				rn.log = append(rn.log, entry)
			}
		} else {
			rn.log = append(rn.log, entry)
		}
	}

	// Advance commitIndex toward the leader's commit index, but never past the
	// last new entry known to exist locally from this AppendEntries.
	if args.LeaderCommit > rn.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewIndex {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = lastNewIndex
		}
		rn.applyCond.Broadcast() // Wake up the apply loop
	}

	return AppendEntriesReply{Term: rn.currentTerm, Success: true}, remote.RemoteError{}
}

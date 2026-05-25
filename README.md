# Team Information
- **Name:** R Yeeshu Dhurandhar, Abhiram Gottumukkala
- **AndrewID:** rdhurand, agottumu
- **Course:** 14-736 Distributed Systems: Techniques, Infrastructure, and Services
- **Mentor:** Prof. Patrick Tague 

# Lab 3: Hierarchical Key-Value Cluster (HKVC)

## Overview

This project implements a **Hierarchical Key-Value Cluster (HKVC)** in Go. The system combines ideas from a distributed file system and a distributed key-value store:

- Data is organized into a hierarchy of directories
- Each directory may contain both subdirectories and ordinary key-value entries
- Different directories may be managed by different **Raft groups**
- Clients interact with the system through an **HTTP API**
- Cluster participants coordinate using the custom **remote RPC** library and the **Raft** implementation from the previous lab

The implementation is divided into three main packages:

- `remote`: a lightweight TCP-based remote method invocation library
- `raft`: a simplified Raft consensus implementation
- `hkvc`: the hierarchical key-value cluster built on top of `remote` and `raft`

---

## Directory Structure

```text
.
├── README.md
├── Makefile
├── hkvc.md
├── hkvc/
│   └── hkvc.go
├── raft/
│   └── raft.go
└── remote/
    ├── callee.go
    ├── caller.go
    └── common.go
```

---

## High-Level Design

### 1. `remote` Package

The `remote` package provides a minimal RPC-style communication library over TCP.

It supports:

- Caller stubs generated from a struct of function fields
- Callee stubs that expose methods over TCP
- `gob`-based serialization
- Optional lossy or delayed communication using `LeakySocket`

This package is used by:

- Raft peers to communicate with each other
- The test controller to manage HKVC participants
- HKVC participants when using internal helper endpoints and control interfaces

### 2. `raft` Package

The `raft` package implements the subset of the Raft protocol needed for the lab:

- Leader election
- Heartbeats
- Log replication
- Commitment
- Activation and deactivation for fault simulation
- Delivery of committed commands through `ApplyMsg`

It does **not** implement:

- Persistence
- Snapshots
- Log compaction
- Membership changes

Each HKVC participant may host multiple local Raft peers, one for each Raft group that includes that participant.

### 3. `hkvc` Package

The `hkvc` package implements the actual hierarchical key-value cluster.

Its responsibilities include:

- Maintaining the global directory hierarchy
- Assigning newly created directories to Raft groups
- Storing ordinary key-value contents inside directories
- Exposing the public HTTP client API
- Translating client requests into replicated Raft commands
- Handling duplicate requests and out-of-order client sequence numbers

---

## HKVC State Organization

The HKVC state is split into two layers.

### Directory Topology

The global directory tree is coordinated through **base group 0**.

This includes:

- Directory creation
- Directory deletion
- Path routing
- Metadata for directories

This design ensures that every participant can converge on a consistent view of the directory hierarchy.

### Directory-Local Key/Value Contents

Ordinary keys stored inside a directory are coordinated through the **Raft group assigned to that directory**.

This includes:

- Listing a directory's immediate contents
- Getting a key
- Setting a key
- Deleting a key
- Metadata for ordinary keys

Each directory therefore acts like a local key-value namespace managed by its assigned group.

---

## Client API

Each participant exposes a single HTTP server with the following endpoints:

| Endpoint | Description |
|---|---|
| `/list` | Lists immediate subdirectories and keys in a directory |
| `/get_metadata` | Returns metadata about a directory or key |
| `/get` | Returns the value of a key |
| `/set` | Creates or updates a key |
| `/create` | Creates a subdirectory |
| `/delete` | Deletes a key or subdirectory subtree |

All requests must use **POST** and carry **JSON request bodies**.

### `/list`

Lists immediate subdirectories and keys in a directory.

- **Request type:** `DirectoryRequest`
- **Success response:** `ListResponse`

### `/get_metadata`

Returns metadata about either:

- The directory itself using key `"."`
- A subdirectory inside a directory
- An ordinary key inside a directory

- **Request type:** `KeyRequest`
- **Success response:** `MetadataResponse`

### `/get`

Returns the value of a key in a directory.

- **Request type:** `KeyRequest`
- **Success response:** `KeyValueMessage`

### `/set`

Creates or updates an ordinary key inside a directory.

- **Request type:** `KeyValueMessage`
- **Success response:** `KeySuccessResponse`

### `/create`

Creates a subdirectory inside an existing directory.

- **Request type:** `KeyRequest`
- **Success response:** `KeySuccessResponse`

### `/delete`

Deletes either an ordinary key inside a directory, or a subdirectory and its full subtree.

- **Request type:** `KeyRequest`
- **Success response:** `KeySuccessResponse`

---

## Internal Helper Endpoints

The implementation also exposes several internal-only HTTP endpoints used for inter-participant coordination:

- `/_hkvc_internal/route`
- `/_hkvc_internal/commit_group`
- `/_hkvc_internal/leader_status`
- `/_hkvc_internal/dir_exists`

These endpoints are **not** part of the public client API. They are used internally to:

- Discover which group owns a directory
- Forward a command to the leader of a specific group
- Discover group leaders
- Confirm visibility of a newly created directory before returning success

---

## Client Sequencing and Idempotence

Each public request includes:

- `client_id`
- `seq_number`

The implementation maintains per-group request history for each client.

| Condition | Behavior |
|---|---|
| Strictly larger sequence number | Processed normally |
| Repeats most recent sequence number | Original response returned again |
| Smaller sequence number | Rejected with `HKVCMsgOutOfSequenceError` |
| Identical request still in flight | Duplicate is attached to existing pending waiter |

This ensures correctness under retries and partial failures.

---

## Concurrency Model

### In `remote`

- Each incoming connection is handled in its own goroutine
- Reflection is used to dispatch the requested method call

### In `raft`

- Each Raft peer has a mutex protecting shared state
- Elections and append RPCs are performed concurrently
- Committed commands are sent to the state machine through `ApplyMsg`

### In `hkvc`

- One apply loop is launched per local Raft group
- A single participant mutex protects the replicated HKVC state:
  - Directory tree
  - Per-directory key/value storage
  - Deduplication history
  - Pending request waiters

---

## Important Design Choices

### Group 0 manages directory structure

All directory topology changes are serialized through base group 0. This makes the hierarchy globally consistent and avoids conflicting directory modifications.

### Directory contents are owned by assigned groups

A directory stores a `GroupID`, and that group is responsible for the directory's ordinary keys.

### Round-robin group assignment for new directories

New directories are assigned to groups using a simple round-robin policy across the sorted group list.

### Read-like operations still go through Raft

This implementation maps observable client operations through Raft so that the response is based on committed state and sequence handling remains consistent.

---

## Building and Running Tests

```bash
# Run final tests
make final

# Run checkpoint tests
make checkpoint

# Run all available tests
make all
```

> The exact targets may depend on the provided Makefile. Use the supplied `make` targets rather than invoking `go test` manually.

---

## Generating Documentation

The project is documented with Go doc comments so that generated package documentation is readable. If your Makefile includes a `docs` target:

```bash
make docs
```

---

## Package Summaries

### `remote`

Implements:

- `CallerStubCreator`
- `NewCalleeStub`
- `LeakySocket`
- `gob`-based request and reply encoding

Used for:

- Controller-to-participant control calls
- Raft peer-to-peer calls

### `raft`

Implements:

- `NewRaftPeer`
- `RequestVote`
- `AppendEntries`
- `NewCommand`
- `Activate`, `Deactivate`, `Terminate`
- `GetStatus`

Used for:

- Replicated command logs
- Leader election
- Commitment and apply delivery

### `hkvc`

Implements:

- `NewHKVCParticipant`
- The public HTTP API
- Internal request forwarding
- Directory hierarchy management
- Per-directory ownership and storage
- Client sequencing and replay behavior

---

## Limitations

This implementation is intentionally scoped to the lab requirements and does not attempt to be a production system.

Known limitations include:

- No persistence across process termination
- No snapshots or log compaction
- No dynamic reconfiguration of Raft groups
- Metadata tags exist structurally but are not actively manipulated by public endpoints
- The system assumes the custom `remote` and `raft` packages behave correctly according to the earlier labs

---

## Failure Scenarios Not Fully Addressed

A few scenarios are outside the intended scope of the lab or only partially handled:

- Very long network partitions may cause requests to time out before internal convergence completes
- Repeated retries across different participants rely on the leader and group routing stabilizing
- There is no background repair for state beyond what Raft replication naturally provides
- There is no persistent recovery after full process restart

---

# References

We used the following references while writing and understanding the code:

- Raft paper for implementation details
- Go documentation: https://go.dev/doc/
- Delve debugger for debugging: https://github.com/go-delve/delve

---

# Use of AI Tools

We used AI tools in a limited support role for the following tasks:

- To understand the test cases
- To understand error messages and debugging strategies
- To learn some Go language syntax and semantics used in this lab
- To get syntax help for Go language, for example:
  - how to use mutexes
  - how to read from a TCP connection
  - how to write to a TCP connection
- To get guidance on how comments and documentation can be structured so that generated documentation is clean and readable
- To paraphrase certain sections of the `README.md` file and comments in the code files
- To convert normal text into markdown format
# Hierarchical Key-Value Cluster API Specification

This document describes the setup requirements for each participant in the hierarchical key-value cluster, how the participants will interact with each other within the cluster, and how external client processes will interact with the cluster from outside.


## What is a Hierarchical Key-Value Cluster?

In short, a hierarchical key-value cluster (HKVC) is a hybrid design that incorporates aspects of a distributed file system and aspects of a distributed hash table.  The *hierarchical* part of the design is that items are stored in the cluster in a hierarchy of directories, similar to a distributed file system.  The *key-value* part of the design is that each directory contains a collection of key-value entries.  While key-value storage is highly valued for its near-constant lookup time, it does not consider dependencies or relationships among keys when deciding which keys will be assigned to which cluster participants for storage.  In particular, when a collection of keys all need to be accessed for a particular service, a sharded distributed hash table may require different key requests to be directed to different storage servers.  In HKVC, keys are grouped into directories to capture these dependencies, and key-value entries stored in the same directory can be assigned to the same cluster participant for storage, which can dramatically reduce the overhead of lookup and access times for the needed data.  Another potential benefit of HKVC compared to DHT is the division of keys into distinct directories may reduce the likelihood of hash collision and the associated overhead.

In our HKVC model, each data entry in the system will be identified by a directory and a key, so a `get` request will have the form `get(directory, key)`, where the directory is a string that may include multiple tokens separated by forward slashes, and a key can be any suitable string.  Keys are allows to reside at any level of the hierarchy, so a single directory may hold both subdirectories and key-value data.

As an simple example, suppose we have a HKVC that stores data in the following directory structure:
```
/
├── a
|   ├── d
|   └── e
├── b
|   ├── a
|   └── b
└── c
    ├── c
    └── d
```
In this example, it would make sense to ensure that all keys in directory `/a/d` are stored by the same participant (or replicated across the same group of participants), and all keys in directory `/c/d` are stored by the same participant (group).  In other words, the directory structure provides a natural guide for how the keys should be allocated across cluster participants.  The simplified lookup capability, indexed only by the directory name, allows a client to identify the participant (group) storing all keys within a particular directory in a single lookup request, without needing to query a specific `key`.

Note that a HKVC with a single directory is a DHT, and a HKVC with file names as keys is a DFS.


## Key-Value Cluster Participant

A participant in our key-value cluster will include several different components and interfaces for different uses.  Here's a high-level summary to help you understand:

* _Raft instances_: participants will interact with each other to provide strong replication properties using Raft.  A single participant may have multiple Raft instances, as it may be replicating multiple independent collections of key-value data, where not every cluster participant is a member of every Raft group.

* _Raft remote interfaces_: as in the previous lab, each Raft instance will include a callee stub to accept remote calls from other participants' Raft instances and a collection of caller stubs to send remote calls to other participants' Raft interfaces; each Raft instance will also have a callee stub to accept remote calls from a Controller, but the Control interface will be slightly modified in this lab from the previous one.

* _Key-value storage_: each cluster participant will have at least one (in-memory) key-value storage data structure, taking the role of the state machine that is collectively managed by the Raft instances.  The data managed by different Raft groups is guaranteed to use disjoint groups of keys, so you can choose whether you want to store all the data in a single data structure or maintain a separate data structure per Raft group.

* _Client API_: each cluster participant will expose a collection of HTTP API endpoints that allow clients to interact with the Key-Value Cluster using higher-level interactions than what the cluster participants use to interact with each other. The API will partially abstract the underlying functionality of Raft and the replicated data structures.

* _Integration logic_: each participant must incorporate an integration layer between the client-facing APIs and the Raft algorithm functionality, possibly including the need for additional inter-participant messaging to ensure the desired properties of the resulting service from the client perspective.


## Cluster Deployment

Similar to the previous lab, the `HKVCController` will handle the task of deploying and configuring the HKVC participants, but in this case, it will be providing additional details for the possibly multiple Raft groups existing across the cluster and the address information needed for the client APIs. The _base group_ of Raft peers will always include all HKVC participants, and this is important to guarantee conflicts can be avoided when creating or deleting directories in the hierarchy.  To allow arbitrary sharding of the collection of key-value data stores arranged within the directory hierarchy, the `HKVCController` can create an arbitrary number of additional Raft groups, which we assume all have the same number of participants, for simplicity.

Creating a `HKVCController` instance thus requires three parameters: the number of participants in the HKVC, the number of additional Raft groups `addlGroups` to create in addition to the base group, and the size of each additional Raft group to create.  The controller will randomly allocate Raft peers to groups and populate a `map[int][]int` map which maps each group identifier `groupID` to the list of participants in the group, noting that the base group is included with `groupID = 0`.

The `HKVCController` also performs the task of generating unique identifiers and port number allocations for all HKVC participants.  This information is used to populate a list of `HKVCSetupInfo` data structures, each containing a participant's unique ID, client interface address, control interface address, and collection of Raft interface addresses.  Since each participant may be a member of up to `1 + addlGroups` Raft groups, it needs to expose this number of ports to host its Raft peer callee stubs.  The client and control intrface addreses are specified as `ip:port` strings, and the Raft group addresses for a single peer are put into a `map[int]string` map that is indexed by the `groupID` of each group, where each mapped value is again an `ip:port` string.  Not all port numbers will necessarily be used, as Raft groups are randomly selected.

For the `HKVCController` used in the test code, all of the interface addresses correspond to the `localhost` IP address, since all of them are run on a single machine.

Once the controller has generated all of this information, it spawns each participant by calling the `NewHKVCParticipant` function in a separate go routine, giving a participant the entire `HKVCSetupInfo` data structure, the participant's index into this data structure, and the map describing all Raft groups.  Once spawned, the controller creates its caller stubs to interact with the HKVC participants using remote calls to their control interface.


## Client API Specification

Next, we'll detail the various HTTP API endpoints that each cluster participant must expose to external clients through the Client API.  Each endpoint is specified with its endpoint name, input request message structure, and the details of any response message types that it can respond with.  All request and response messages are encoded using JSON, and every response must have an accompanying HTTP response code.  The API endpoints should only support POST requests, and only the relevant Raft leader should accept new commands from clients (with one exception, described later).

Most of the request message types used by a client will include a sequence number, which should be a strictly monotonically increasing integer to allow the cluster participants to differentiate between new commands and repeated requests (i.e., to address the failure case where the client doesn't get the initial response).  A cluster participant should handle requests based on sequence numbers as follows:
* If the sequence number in a request is strictly larger than that of the previous message received from the client, it should continue to process this message.
* If the client repeats a message with the same sequence number, the initial request has already been processed, and the participant attempted to send the response to the client, then the participant should respond with the original response (if available) or what the response would be using the current system state.
* If the client repeats a message that is still pending (i.e., it's still being handled by Raft), then the participant can disregard the new command, but it may need to take note of the connection information for the client, in case their initial connection was terminated.
* If the client repeats a sequence number that is strictly less than the largest previous message, the participant should treat this as an error, to be discussed shortly.

Of particular note, there are a variety of different request and response message structures that are used by different endpoints, but all error responses to the client will have the same basic structure, referred to as an `HKVCErrorResponse`.  Since the error response structure applies across all endpoints that can return errors, we'll cover that first before going into the specifics of any individual endpoint. All error responses will have the message structure of

```json
{
    "error_type": "ErrorType",
    "error_info": "Optional verbose description of the error that occurred.",
    "client_id":  "Unique identifier per client."
}
```

Each endpoint specifies what specific `ErrorType`s can result from calling each endpoint.  While many of the error types and status codes are specific to the endpoint, there are three errors that all endpoints can return for the same reasons.  These are:
* Error type `HKVCInvalidRequestError` with status code `400 Bad Request` is returned on receipt of any request type other than POST, any request that cannot be decoded into the expected format, or any request with invalid arguments.
* Error type `HKVCNonRaftLeaderError` with status code `403 Forbidden` is returned by a participant who receives a request they cannot handle because they are not the relevant Raft group leader.
* Error type `HKVCMsgOutOfSequenceError` with status code `406 Not Acceptable` is returned on receipt of a message that has a strictly smaller sequence number than previous messages from this client.

As a general rule, all directory paths will be encoded as sequences of string tokens, each preceded by a forward slash (`/`), following typical Unix path conventions.  Of particular note, any number of sequential forward slashes should collapse to a single forward slash, and a terminating forward slash has no effect. For example, the three paths `////a//b`, `/a/b/`, and `/a/b` are all valid paths referring to the same directory.

Each of the following subsections details a single HTTP API endpoint, noting that all endpoints are ex posed on a single TCP port, referred to as the participant's *client interface*.  In total, the client interface comprises a total of six (6) endpoints named `list`, `get_metadata`, `get`, `set`. `create`, and `delete`.  The complete specification of each of these endpoints follows.


### Client Interface List Endpoint

The `/list` endpoint is used by a client to explore the contents of a given directory.  The client will create a `DirectoryRequest` containing a full directory path as a string, which will then be encoded into a JSON message.  An example JSON message is
```json
{
    "directory": "/a/b/c",
    "seq_number": 4,
    "client_id":  "Unique identifier per client."
}
```


#### Success Response Message

The response to a valid, successful request to the `/list` endpoint will include the HTTP `200 OK` status code along with a JSON response containing a list of subdirectories and keys that immediately reside in the requested directory.  The JSON message will decode into a `ListResponse` data structure.  An example JSON response is
```json
{
    "directory": "/a/b/c",
    "list": [
        "dir0",
        "dir1",
        "key0",
        "key1"
    ],
    "client_id":  "Unique identifier per client."
}
```
noting that there is no indication of which list elements are subdirectories and which are keys identifying stored data, though this information can be obtained through subsequent calls.


#### Error Response Messages

There are several error cases that can be encountered when a `DirectoryRequest` is sent to the `/list` endpoint, and the system will respond to each error with a different response code, error type, and message.

If the JSON message does not decode into a proper `DirectoryRequest` data structure or if the string contained in the `directory` field is not a valid directory string, the system should respond to the client with an HTTP `400 Bad Request` status code, an error of type `HKVCInvalidRequestError`, and a suitable error description.

If the `directory` string is valid but does not exist as a directory stored by the receiving participant, then it should respond with an HTTP `404 Not Found` status code, an error of type `HKVCDirectoryNotFoundError`, and a suitable error description.



### Client Interface Get-Metadata Endpoint

The `/get_metadata` endpoint is used by a client to request the metadata and status of either a specific directory or a particular key within a specific directory.  The client will create a `KeyRequest` message data structure containing both a full directory path and a key as strings, which will then be encoded into a JSON message.  The `key` field of this request can refer to either a subdirectory or a proper key contained immediately within the path indicated by the `directory` field (the interpretation should be clear to the receiving participant).  An example JSON message is
```json
{
    "directory": "/a/b/c",
    "key": "aW5pNzM2ZGZz",
    "seq_number": 4,
    "client_id":  "Unique identifier per client."
}
```


#### Success Response Message

The response to a valid request to the `/get_metadata` endpoint will include the HTTP `200 OK` status code along with a JSON response containing a list of metadata fields stored with the given key in the target directory. As described previously, the list of possible metadata fields present in a `MetadataResponse` data structure includes:
* the `directory` and `key` fields used to identify the key-value entry
* a boolean `is_directory` field that indicates whether the `key` itself corresponds to a directory or a proper key
* an integer `size` field that holds the size of the stored value in bytes if `is_directory` is false, or the value `-1` if `is_directory` is true
* an integer `version` field indicating the version number of the key-value entry
* a list `p_addr_list` of participant addresses in the group managing this key-value entry, each of which includes an ip address and port number separated by a colon
* an integer `leader_index` identifying the participant in `p_addr_list` that is the leader at the time of the response
* a list `tags` of strings that serve as keywords or labels describing the key-value entry

An example JSON response is
```json
{
    "directory": "/a/b/c",
    "key": "aW5pNzM2ZgZz",
    "is_directory": false,
    "size", 48,
    "version": 1935485,
    "p_addr_list": [
        ip0:port0,
        ...,
        ipk:portk
    ],
    "leader_index": 0,
    "tags": [
        "tag0",
        ...,
        "tagm"
    ],
    "client_id":  "Unique identifier per client."
}
```


#### Error Response Messages

There are several error cases that can be encountered when a `KeyRequest` is sent to the `/get_metadata` endpoint, and the system will respond to each error with a different response code, error type, and message.

If the JSON message does not decode into a proper `KeyRequest` data structure, the string contained in the `directory` field is not a valid directory string, or the `key` field is empty/null, the system should respond to the client with an HTTP `400 Bad Request` status code, an error of type `HKVCInvalidRequestError`, and a suitable error description.

If the `directory` string is valid but does not exist as a directory stored by the receiving participant, then it should respond with an HTTP `404 Not Found` status code, an error of type `HKVCDirectoryNotFoundError` error, and a suitable error description.

If the directory is present but the `key` is not present within it, then the participant should respond with an HTTP `404 Not Found` status code, an error of type `HKVCKeyNotFoundError`, and a suitable error description.



### Client Interface Get Endpoint

The `/get` endpoint is used by a client to request the value associated with a particular key within a given directory.  The client will create a `KeyRequest` message data structure containing both a full directory path and a key as strings, which will then be encoded into a JSON message.  An example JSON message is
```json
{
    "directory": "/a/b/c",
    "key": "aW5pNzM2ZGZz",
    "seq_number": 4,
    "client_id":  "Unique identifier per client."
}
```


#### Success Response Message

The response to a valid request to the `/get` endpoint will include the HTTP `200 OK` status code along with a JSON response containing the directory, the key, and the stored value as strings, where the value string is the result of base64 encoding the stored sequence of bytes (e.g., a byte array or slice), since JSON doesn't directly support byte sequences.  The JSON message will be decoded into a `KeyValueMessage` data structure.  An example JSON message response is
```json
{
    "directory": "/a/b/c",
    "key": "aW5pNzM2ZGZz"
    "value": "dGhpc2xhYmlzc29tdWNoZnVu",
    "client_id":  "Unique identifier per client."
}
```


#### Error Response Messages

There are several error cases that can be encountered when a `KeyRequest` is sent to the `/get` endpoint, and the system will respond to each error with a different response code, error type, and message.

If the JSON message does not decode into a proper `KeyRequest` data structure, the string contained in the `directory` field is not a valid directory string, or the `key` field is empty/null, the system should respond to the client with an HTTP `400 Bad Request` status code, an error of type `HKVCInvalidRequestError`, and a suitable error description.

If the `directory` string is valid but does not exist as a directory stored by the receiving participant, then it should respond with an HTTP `404 Not Found` status code, an error of type `HKVCDirectoryNotFoundError` error, and a suitable error description.

If the directory is present but the `key` is not present within it, then the participant should respond with an HTTP `404 Not Found` status code, an error of type `HKVCKeyNotFoundError`, and a suitable error description.



### Client Interface Set Endpoint

The `/set` endpoint is used by a client to store a value associated with a particular key within a given directory.  The request message format/structure used for `/set` requests is the same `KeyValueMessage` used for a `/get` response (which is why its name doesn't include the words request or respones like the others).


#### Success Response Message

The response to a valid request to the `/set` endpoint will include the HTTP `200 OK` status code if the receiving participant overwrote a previous value for the given directory and key or the HTTP `201 Created` status code if the key-value pair did not previously exist within the specified directory.  In either case, the participant will respond to the `/set` request with a message fitting the `KeySuccessResponse` message, which echoes the directory and key fields along with a boolean field with value `true` to indicate success.  An example JSON message response is
```json
{
    "directory": "/a/b/c",
    "key": "aW5pNzM2ZGZz"
    "success": true,
    "client_id":  "Unique identifier per client."
}
```


#### Error Response Messages

There are several error cases that can be encountered when a `KeyValueMessage` is sent to the `/set` endpoint, and the system will respond to each error with a different response code, error type, and message.

If the JSON message does not decode into a proper `KeyValueMessage` data structure, the string contained in the `directory` field is not a valid directory string, or the `key` field is empty/null, the system should respond to the client with an HTTP `400 Bad Request` status code, an error of type `HKVCInvalidRequestError`, and a suitable error description.

If the `directory` string is valid but does not exist on the receiving participant, then it should respond with an HTTP `404 Not Found` status code, an error of type `HKVCDirectoryNotFoundError` error, and a suitable error description.

If the `directory` string is valid but it refers to a key stored on the receiving participant, then the participant should respond with an HTTP `409 Conflict` status code, an error of type `HKVCConflictExistingKeyError`, and a suitable error description.

If the directory is valid and present but the `key` contains the name of an existing directory within the location specified by the `directory` string, then the participant should respond with an HTTP `409 Conflict` status code, an error of type `HKVCConflictExistingDirectoryError`, and a suitable error description.




### Client Interface Create Endpoint

The `/create` endpoint is used by a client to create a new directory within an existing parent directory (like `mkdir`), by including the new directory's complete path in a `KeyRequest` message, where the `directory` field contains the parent directory and the `key` field contains the name of the directory to be created.  An example JSON message is
```json
{
    "directory": "/a/b/c",
    "key": "d",
    "seq_number": 4,
    "client_id":  "Unique identifier per client."
}
```
which will result in creation of a new directory at path `/a/b/c/d`.



#### Success Response Message

The response to a valid request to the `/create` endpoint will include one of two possible HTTP status codes along with a JSON response containing a boolean value indicating whether the directory was created at the requested path.

If the name in `key` already exists as a subdirectory of the parent indicated in the `directory` field in the participant's local storage, the response will indicate the HTTP `200 OK` status code and include a `KeySuccessResponse` message (similar to that in `/set`), which echoes the directory and key fields along with a boolean field with value `false` to indicate that the target directory already exists and is safe to use.

If the name in `key` does not exist anywhere in the cluster, but its parent directory exists in the participant's local storage, then the participant will create the new directory and respond with the HTTP `201 Created` status code and a `KeySuccessResponse` message with the boolean value `true` to indicate that the target directory was created and can now be used with this cluster participant.


#### Error Response Messages

There are several error cases that can be encountered when a `KeyRequest` is sent to the `/create` endpoint, and the system will respond to each error with a different response code, error type, and message.

If the JSON message does not decode into a proper `KeyRequest` data structure, the string contained in the `directory` field is not a valid directory string, or the `key` field is empty/null, the system should respond to the client with an HTTP `400 Bad Request` status code, an error of type `HKVCInvalidRequestError`, and a suitable error description.

If the `directory` string is valid but does not exist on the receiving participant, then it should respond with an HTTP `404 Not Found` status code, an error of type `HKVCDirectoryNotFoundError` error, and a suitable error description.

If the `directory` string is valid but it refers to a key stored on the receiving participant, then the participant should respond with an HTTP `409 Conflict` status code, an error of type `HKVCConflictExistingKeyError`, and a suitable error description.

Similarly, if the `directory` string is valid and refers to an appropriately existing directory, but the `key` field refers to an existing key (not a subdirectory), then the participant should respond with an HTTP `409 Conflict` status code, an error of type `HKVCConflictExistingKeyError`, and a suitable error description.



### Client Interface Delete Endpoint

The `/delete` endpoint is used by a client to delete either a specific directory (and all contents and subdirectories, like `rm -rf`) or a particular key within a specific directory (like `rm -f`).  The request message structure used for `/delete` requests is the same `KeyRequest` used for `/get_metadata` requests, where the `key` can refer to a subdirectory or a proper key located according to the `directory` field.


#### Success Response Message

The response to a valid request to the `/delete` endpoint will include the HTTP `200 OK` status code along with a `KeySuccessResponse` message, similar to that used for the `/set` endpoint.


#### Error Response Messages

There are several error cases that can be encountered when a `KeyRequest` is sent to the `/delete` endpoint, and the system will respond to each error with a different response code, error type, and message.

If the JSON message does not decode into a proper `KeyRequest` data structure, the string contained in the `directory` field is not a valid directory string, or the `key` field is empty/null, the system should respond to the client with an HTTP `400 Bad Request` status code, an error of type `HKVCInvalidRequestError`, and a suitable error description.

If the `directory` string is valid but does not exist as a directory stored by the receiving participant, then it should respond with an HTTP `404 Not Found` status code, an error of type `HKVCDirectoryNotFoundError` error, and a suitable error description.

If the directory is present but the `key` is not present within it, then the participant should respond with an HTTP `404 Not Found` status code, an error of type `HKVCKeyNotFoundError`, and a suitable error description.


-----

-----

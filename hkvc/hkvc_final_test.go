package hkvc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// encode and send given msg to list endpoint on each port in slice to find who responds back as leader
func findDirLeaderViaList(t *testing.T, msg any, ports []int) int {
	lr := ListResponse{}
	leader := -1
	for i := range len(ports) {
		jsonmsg, e := json.Marshal(msg)
		if e != nil {
			t.Fatal("Error encoding list request: ", e)
		}
		rc, er, err := getResponse(ports[i], "/list", jsonmsg, &lr)
		// skip anyone who is failed or rejects because they're not the leader
		if err != nil || (er != nil && er.ErrorType == NonLeaderError) {
			continue
		}
		if er == nil && rc == http.StatusOK {
			if leader != -1 {
				t.Fatal("Multiple participants claim to be the leader")
			}
			leader = i
		}
	}
	return leader
}

//
// Final tests start here !!!
//

// test correct handling of duplicate and out-of-order client commands
func TestFinal_ClientSequencing(t *testing.T) {
	clusterSize := 3
	ctrl := NewHKVCController(t, clusterSize, 0, 0)
	leader, _ := ctrl.getGroupLeaderCommit(0)
	leaderClientPort := ctrl.basePort + (3+ctrl.nAddlGroups)*leader

	clientName := randSeq(12)

	fmt.Print("Checking for correct handling of commands with repeated sequence number ... ")
	// create a few initial keys to work with
	ksr := KeySuccessResponse{}
	numKeys := 3
	keyList := make([]string, numKeys)
	for k := range numKeys {
		kvm := KeyValueMessage{
			Directory: "/",
			Key:       "key" + strconv.Itoa(k),
			Value:     "value #" + strconv.Itoa(k),
			SeqNumber: k,
			ClientID:  clientName,
		}
		keyList[k] = kvm.Key

		validateResponseToClient(t, kvm, leaderClientPort, "/set", &ksr, "", http.StatusCreated)
	}

	// duplicate of last set message, should repeat same response
	validateResponseToClient(t, KeyValueMessage{Directory: "/", Key: keyList[2], Value: "value #2", SeqNumber: 2, ClientID: clientName}, leaderClientPort, "/set", &ksr, "", http.StatusCreated)

	// outdated set message, should report a sequence error
	validateResponseToClient(t, KeyValueMessage{Directory: "/", Key: keyList[0], Value: "new value #0", SeqNumber: 0, ClientID: clientName}, leaderClientPort, "/set", &ksr, OutOfSequenceError, http.StatusNotAcceptable)

	// issue get commands for the three values we just set
	kvm := KeyValueMessage{}
	numKeys = 3
	for k := range numKeys {
		validateResponseToClient(t, KeyRequest{Directory: "/", Key: keyList[k], SeqNumber: k + 3, ClientID: clientName}, leaderClientPort, "/get", &kvm, "", http.StatusOK)
	}

	// duplicate of last get message, should repeat same response
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: keyList[2], SeqNumber: 5, ClientID: clientName}, leaderClientPort, "/get", &kvm, "", http.StatusOK)

	// outdated get message, should report a sequence error
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: keyList[1], SeqNumber: 4, ClientID: clientName}, leaderClientPort, "/get", &kvm, OutOfSequenceError, http.StatusNotAcceptable)

	// client sends a list command with fresh sequence number
	lr := ListResponse{}
	validateResponseToClient(t, DirectoryRequest{Directory: "/", SeqNumber: 7, ClientID: clientName}, leaderClientPort, "/list", &lr, "", http.StatusOK)

	// duplicate of list command, should repeat same response
	validateResponseToClient(t, DirectoryRequest{Directory: "/", SeqNumber: 7, ClientID: clientName}, leaderClientPort, "/list", &lr, "", http.StatusOK)

	// outdated list command, should report a sequence error
	validateResponseToClient(t, DirectoryRequest{Directory: "/", SeqNumber: 3, ClientID: clientName}, leaderClientPort, "/list", &lr, OutOfSequenceError, http.StatusNotAcceptable)

	// client sends a get_metadata command with fresh sequence number
	mr := MetadataResponse{}
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: keyList[1], SeqNumber: 11, ClientID: clientName}, leaderClientPort, "/get_metadata", &mr, "", http.StatusOK)

	// duplicate of get_metadata command, should repeat same response
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: keyList[1], SeqNumber: 11, ClientID: clientName}, leaderClientPort, "/get_metadata", &mr, "", http.StatusOK)

	// outdated get_metadata command, should report a sequence error
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: keyList[1], SeqNumber: 8, ClientID: clientName}, leaderClientPort, "/get_metadata", &mr, OutOfSequenceError, http.StatusNotAcceptable)

	// client sends a create command with fresh sequence number
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: 13, ClientID: clientName}, leaderClientPort, "/create", &ksr, "", http.StatusCreated)

	// duplicate of create command, should repeat same response
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: 13, ClientID: clientName}, leaderClientPort, "/create", &ksr, "", http.StatusCreated)

	// outdated create command, should report a sequence error
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: 7, ClientID: clientName}, leaderClientPort, "/create", &ksr, OutOfSequenceError, http.StatusNotAcceptable)

	// client sends a delete command with fresh sequence number
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: 15, ClientID: clientName}, leaderClientPort, "/delete", &ksr, "", http.StatusOK)

	// duplicate of delete command, should repeat same response
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: 15, ClientID: clientName}, leaderClientPort, "/delete", &ksr, "", http.StatusOK)

	// outdated delete command, should report a sequence error
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: 7, ClientID: clientName}, leaderClientPort, "/delete", &ksr, OutOfSequenceError, http.StatusNotAcceptable)

	fmt.Println("ok")
}

// test hierarchical key-value store creation using create and set and then validate using list
func TestFinal_CreateHierarchy(t *testing.T) {
	clusterSize := 5
	ctrl := NewHKVCController(t, clusterSize, 0, 0)
	leader, _ := ctrl.getGroupLeaderCommit(0)
	leaderClientPort := ctrl.basePort + (3+ctrl.nAddlGroups)*leader

	clientName := randSeq(12)

	fmt.Print("Checking client use of create command to make directories ... ")
	ksr := KeySuccessResponse{}
	numDirs := 5
	numKeys := 4
	seq := 0

	for d := range numDirs {
		// all of these should return "success: true" and status code 201 Created
		validateResponseToClient(t, KeyRequest{Directory: "/", Key: "d" + strconv.Itoa(d), SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/create", &ksr, "", http.StatusCreated)
		seq++
		if ksr.Directory != "/" || ksr.Key != "d"+strconv.Itoa(d) || !ksr.Success {
			t.Fatal("Response to valid create command does not have expected contents")
		}
	}
	fmt.Println("ok")

	fmt.Print("Checking duplicate create requests are handled correctly ... ")
	for d := range numDirs {
		// all of these should return "success: false" and status code 200 OK
		validateResponseToClient(t, KeyRequest{Directory: "/", Key: "d" + strconv.Itoa(d), SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/create", &ksr, "", http.StatusOK)
		seq++
		if ksr.Directory != "/" || ksr.Key != "d"+strconv.Itoa(d) || ksr.Success {
			t.Fatal("Response to duplicate create command does not have expected contents")
		}
	}
	fmt.Println("ok")

	fmt.Print("Checking set correctly handles keys of same name in different directories ... ")
	for k := range numKeys {
		for d := range numDirs {
			// all of these should return "success: true" and status code 201 Created
			kvm := KeyValueMessage{
				Directory: "/d" + strconv.Itoa(d),
				Key:       "key" + strconv.Itoa(k),
				Value:     fmt.Sprintf("value #%d in d%d", k, d),
				SeqNumber: seq,
				ClientID:  clientName,
			}
			validateResponseToClient(t, kvm, leaderClientPort, "/set", &ksr, "", http.StatusCreated)
			seq++
			if ksr.Directory != "/d"+strconv.Itoa(d) || ksr.Key != "key"+strconv.Itoa(k) || !ksr.Success {
				t.Fatal("Response to valid set command does not have expected contents")
			}
		}
	}
	fmt.Println("ok")

	fmt.Print("Checking sequential set requests are handled correctly ... ")
	for d := range numDirs {
		// all of these should return "success: true" and status code 200 OK
		kvm := KeyValueMessage{
			Directory: "/d" + strconv.Itoa(d),
			Key:       "key0",
			Value:     "new value #0 in d" + strconv.Itoa(d),
			SeqNumber: seq,
			ClientID:  clientName,
		}
		validateResponseToClient(t, kvm, leaderClientPort, "/set", &ksr, "", http.StatusOK)
		seq++
		if ksr.Directory != "/d"+strconv.Itoa(d) || ksr.Key != "key0" || !ksr.Success {
			t.Fatal("Response to subsequent set command does not have expected contents")
		}

		validateResponseToClient(t, KeyRequest{Directory: "/d" + strconv.Itoa(d), Key: "key0", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/get", &kvm, "", http.StatusOK)
		seq++
		if kvm.Value != "new value #0 in d"+strconv.Itoa(d) {
			t.Fatal("Response to subsequent get command does not reflect overwritten value")
		}
	}
	fmt.Println("ok")

	fmt.Print("Checking creation of nested directories ... ")
	dir := "/d0"
	levels := 4
	for l := range levels {
		// all of these should return "success: true" and status code 201 Created
		validateResponseToClient(t, KeyRequest{Directory: dir, Key: "subdir" + strconv.Itoa(l), SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/create", &ksr, "", http.StatusCreated)
		seq++
		if ksr.Directory != dir || ksr.Key != "subdir"+strconv.Itoa(l) || !ksr.Success {
			t.Fatal("Response to valid create command does not have expected contents")
		}
		dir += "/subdir" + strconv.Itoa(l)
	}
	fmt.Println("ok")

	fmt.Print("Checking set and create handle conflicts correctly ... ")
	// this should return a HKVCConflictExistingKeyError error and status code 409 Conflict
	kvmCS := KeyValueMessage{
		Directory: "/d0/key0",
		Key:       "keyThatShouldFail",
		Value:     "value that doesn't matter",
		SeqNumber: seq,
		ClientID:  clientName,
	}
	validateResponseToClient(t, kvmCS, leaderClientPort, "/set", &ksr, ConflictKeyError, http.StatusConflict)
	seq++

	// this should return a HKVCConflictExistingKeyError error and status code 409 Conflict
	validateResponseToClient(t, KeyRequest{Directory: "/d0/key0", Key: "keyThatShouldFail", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/create", &ksr, ConflictKeyError, http.StatusConflict)
	seq++

	// this should return a HKVCConflictExistingDirectoryError error and status code 409 Conflict
	kvmCD := KeyValueMessage{
		Directory: "/d0/subdir0",
		Key:       "subdir1",
		Value:     "value that doesn't matter",
		SeqNumber: seq,
		ClientID:  clientName,
	}
	validateResponseToClient(t, kvmCD, leaderClientPort, "/set", &ksr, ConflictDirError, http.StatusConflict)
	seq++

	// this should return a HKVCConflictExistingKeyError error and status code 409 Conflict
	krCDK := KeyRequest{
		Directory: "/d0",
		Key:       "key0",
		SeqNumber: seq,
		ClientID:  clientName,
	}
	validateResponseToClient(t, krCDK, leaderClientPort, "/create", &ksr, ConflictKeyError, http.StatusConflict)
	seq++

	fmt.Println("ok")
}

// test client requests aren't responded to until past commands are all applied or otherwise handled
func TestFinal_RespondAfterCommit(t *testing.T) {
	clusterSize := 5
	ctrl := NewHKVCController(t, clusterSize, 0, 0)
	leader, _ := ctrl.getGroupLeaderCommit(0)
	leaderClientPort := ctrl.basePort + (3+ctrl.nAddlGroups)*leader

	clientName := randSeq(12)

	fmt.Print("Checking parallel set and list commands are handled sequentially ... ")
	numKeys := 20
	var mu sync.Mutex
	var mu2 sync.Mutex
	var wg sync.WaitGroup
	seq := 0
	successes := 0

	wg.Add(numKeys)

	// multiple somewhat-concurrent set commands using a shared sequence number
	for range numKeys {
		go func() {
			for {
				// try to synchronize order of set commands
				mu.Lock()
				k := seq
				seq++
				key := "key" + strconv.Itoa(k)
				value := "value #" + strconv.Itoa(k)
				mu.Unlock()

				// client sends valid set command
				ksr := KeySuccessResponse{}

				// validation is done separately below, so last two args are nil but return values are captured
				rc, er := validateResponseToClient(t, KeyValueMessage{Directory: "/", Key: key, Value: value, SeqNumber: k, ClientID: clientName}, leaderClientPort, "/set", &ksr, "", 0)
				if (er != nil && er.ErrorType == OutOfSequenceError) || rc == http.StatusNotAcceptable {
					// syncronization failed, so try again
					continue
				}

				mu2.Lock()
				successes++
				s := successes
				mu2.Unlock()

				if er != nil || rc != http.StatusCreated {
					t.Fatal("Response to valid set command returned error type or incorrect status code")
				}
				if ksr.Directory != "/" || ksr.Key != key || !ksr.Success {
					t.Fatal("Response to valid set command does not have expected contents")
				}

				// check leader hasn't changed, commit index is at least s
				curLead, comIdx := ctrl.getGroupLeaderCommit(0)
				if curLead != leader {
					t.Fatal("Leader changed unexpectedly")
				}
				if comIdx < s {
					t.Fatal("Client received response before leader committed the request")
				}

				break
			}
			wg.Done()
		}()
	}

	// wait for all client requests to finish
	wg.Wait()

	fmt.Println("ok")
}

// test numerous client requests interleaved with leader failures, trying to find leader using data
// fetched from get_metadata, making sure that everything remains consistent and all commands are handled
func TestFinal_FaultyLeaders(t *testing.T) {
	clusterSize := 5
	ctrl := NewHKVCController(t, clusterSize, 0, 0)
	leader, _ := ctrl.getGroupLeaderCommit(0)
	leaderClientPort := ctrl.basePort + (3+ctrl.nAddlGroups)*leader

	clientName := randSeq(12)

	fmt.Print("Checking metadata contents match expected values ... ")

	ksr := KeySuccessResponse{}
	seq := 0

	// should return "success: true" and status code 201 Created
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/create", &ksr, "", http.StatusCreated)
	seq++
	if ksr.Directory != "/" || ksr.Key != "dir" || !ksr.Success {
		t.Fatal("Response to valid create command does not have expected contents")
	}

	numModifies := 8

	// should return "success: true" and status code 201 Created
	validateResponseToClient(t, KeyValueMessage{Directory: "/dir", Key: "modkey", Value: "0", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/set", &ksr, "", http.StatusCreated)
	seq++
	if ksr.Directory != "/dir" || ksr.Key != "modkey" || !ksr.Success {
		t.Fatal("Response to valid set command does not have expected contents")
	}

	for m := range numModifies {
		// should return "success: true" and status code 200 OK
		validateResponseToClient(t, KeyValueMessage{Directory: "/dir", Key: "modkey", Value: strconv.Itoa(m), SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/set", &ksr, "", http.StatusOK)
		seq++
		if ksr.Directory != "/dir" || ksr.Key != "modkey" || !ksr.Success {
			t.Fatal("Response to valid set command does not have expected contents")
		}
	}

	addrList := make([]string, clusterSize)
	for i := range clusterSize {
		addrList[i] = ctrl.setupInfo[i].ClientAddr
	}

	// get_metadata request to validate version, addresses, leader
	mr := MetadataResponse{}
	validateResponseToClient(t, KeyRequest{Directory: "/dir", Key: "modkey", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/get_metadata", &mr, "", http.StatusOK)
	seq++
	if mr.Directory != "/dir" || mr.Key != "modkey" || mr.IsDirectory {
		t.Fatal("Metadata response has incorrect directory, key, or is_directory value")
	}
	if mr.Version < uint64(numModifies) {
		t.Fatal("Metadata response has invalid version number")
	}
	if !sameStringSlices(mr.PAddrList, addrList) {
		t.Fatal("Metadata response has incorrect participant address list values")
	}
	if mr.LeaderIdx != leader {
		t.Fatal("Metadata response has incorrect leader index")
	}
	fmt.Println("ok")

	fmt.Print("Checking clients can find new leader with consistent state... ")
	failureRounds := 4

	for f := range failureRounds {
		leader, _ = ctrl.getGroupLeaderCommit(0)
		leaderClientPort = ctrl.basePort + (3+ctrl.nAddlGroups)*leader
		ctrl.disconnect(leader)
		time.Sleep(WaitForRaft)

		kvm := KeyValueMessage{}
		for range numModifies {
			jsonmsg, eg := json.Marshal(KeyRequest{Directory: "/dir", Key: "modkey", SeqNumber: seq, ClientID: clientName})
			seq++
			if eg != nil {
				t.Fatal("Error encoding get request: ", eg)
			}
			_, _, errg := getResponse(leaderClientPort, "/get", jsonmsg, &kvm)
			if errg == nil {
				t.Fatal("Failed leader accepted an HTTP get request")
			}
		}

		// client can find the leader by calling any api and checking for error
		clientLeader := -1
		for i := range clusterSize {
			addrTokens := strings.Split(mr.PAddrList[i], ":")
			port, errport := strconv.Atoi(addrTokens[len(addrTokens)-1])
			if errport != nil {
				t.Fatal("Error extracting port number from string: ", errport)
			}

			jsonmsg, eg := json.Marshal(KeyRequest{Directory: "/dir", Key: "modkey", SeqNumber: seq, ClientID: clientName})
			seq++
			if eg != nil {
				t.Fatal("Error encoding get request: ", eg)
			}
			rcg, erg, errg := getResponse(port, "/get", jsonmsg, &kvm)
			if i == leader && errg == nil {
				t.Fatal("Failed leader accepted an HTTP get request")
			}
			// skip anyone who is not the new leader
			if i == leader || (erg != nil && erg.ErrorType == NonLeaderError) {
				continue
			}
			if erg == nil && rcg == http.StatusOK {
				if clientLeader != -1 {
					t.Fatal("Multiple participants claim to be the leader")
				}
				clientLeader = i
			}
		}
		if clientLeader == -1 {
			t.Fatal("New leader not found after failure of previous leader")
		}

		newLeader, comIdx := ctrl.getGroupLeaderCommit(0)
		if newLeader == leader || newLeader == -1 || clientLeader != newLeader {
			t.Fatal("Client discovery of new leader is inconsistent with state reported to controller")
		}
		// prev leader must have committed at least 1 create, 1+numModifies set, and 1 get_metadata request
		// new leader must have committed at least 1+f get requests
		if comIdx < 4+f+numModifies {
			t.Fatal("New leader has not committed all commands from previous leader")
		}

		ctrl.connect(leader)
		time.Sleep(WaitForRaft) // in case prev leader returns and causes a new election
	}

	fmt.Println("ok")
}

// test directory and key-value state is correctly managed when keys and directories are deleted
// and subsequently redefined. all return values and version numbers should reflect freshness of
// redefined contents, not maintained across deletion
func TestFinal_DeleteAndRebuild(t *testing.T) {
	clusterSize := 5
	ctrl := NewHKVCController(t, clusterSize, 0, 0)
	leader, _ := ctrl.getGroupLeaderCommit(0)
	leaderClientPort := ctrl.basePort + (3+ctrl.nAddlGroups)*leader

	clientName := randSeq(12)

	fmt.Print("Checking that keys are properly deleted along with their metadata ... ")

	ksr := KeySuccessResponse{}
	mr := MetadataResponse{}
	kvm := KeyValueMessage{}
	numModifies := 8
	seq := 0

	// should return "success: true" and status code 201 Created
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "d", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/create", &ksr, "", http.StatusCreated)
	seq++
	if ksr.Directory != "/" || ksr.Key != "d" || !ksr.Success {
		t.Fatal("Response to valid create command does not have expected contents")
	}

	// get_metadata request to validate is_directory and contents
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "d", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/get_metadata", &mr, "", http.StatusOK)
	seq++
	if mr.Directory != "/" || mr.Key != "d" || !mr.IsDirectory {
		t.Fatal("Metadata response has incorrect directory, key, or is_directory value")
	}
	if mr.LeaderIdx != leader {
		t.Fatal("Metadata response has incorrect leader index")
	}

	// should return "success: true" and status code 201 Created
	validateResponseToClient(t, KeyValueMessage{Directory: "/d", Key: "k", Value: "v", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/set", &ksr, "", http.StatusCreated)
	seq++
	if ksr.Directory != "/d" || ksr.Key != "k" || !ksr.Success {
		t.Fatal("Response to valid set command does not have expected contents")
	}

	for m := range numModifies {
		// should return "success: true" and status code 200 OK
		validateResponseToClient(t, KeyValueMessage{Directory: "/d", Key: "k", Value: "v" + strconv.Itoa(m), SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/set", &ksr, "", http.StatusOK)
		seq++
		if ksr.Directory != "/d" || ksr.Key != "k" || !ksr.Success {
			t.Fatal("Response to valid set command does not have expected contents")
		}
	}

	validateResponseToClient(t, KeyRequest{Directory: "/d", Key: "k", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/get_metadata", &mr, "", http.StatusOK)
	seq++
	if mr.Directory != "/d" || mr.Key != "k" || mr.IsDirectory {
		t.Fatal("Metadata response has incorrect directory, key, or is_directory value")
	}
	if mr.Version < uint64(numModifies) {
		t.Fatal("Metadata response has incorrect version number")
	}
	if mr.LeaderIdx != leader {
		t.Fatal("Metadata response has incorrect leader index")
	}

	kVersion := mr.Version

	// delete request on key /d/k should succeed with return code 200 OK
	validateResponseToClient(t, KeyRequest{Directory: "/d", Key: "k", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/delete", &ksr, "", http.StatusOK)
	seq++
	if ksr.Directory != "/d" || ksr.Key != "k" || !ksr.Success {
		t.Fatal("Response to valid delete command does not have expected contents")
	}

	// validation is done separately below, so last two args are nil but return values are captured
	rc, er := validateResponseToClient(t, KeyRequest{Directory: "/d", Key: "k", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/get", &kvm, "", 0)
	seq++
	if er == nil || rc == http.StatusOK {
		t.Fatal("Get endpoint accepted request for deleted key without error")
	}

	// validation is done separately below, so last two args are nil but return values are captured
	rc, er = validateResponseToClient(t, KeyRequest{Directory: "/d", Key: "k", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/get_metadata", &mr, "", 0)
	seq++
	if er == nil || rc == http.StatusOK {
		t.Fatal("Get-metadata endpoint accepted request for deleted key without error")
	}

	// create same key that was just deleted, should return "success: true" and status code 201 Created
	validateResponseToClient(t, KeyValueMessage{Directory: "/d", Key: "k", Value: "v", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/set", &ksr, "", http.StatusCreated)
	seq++
	if ksr.Directory != "/d" || ksr.Key != "k" || !ksr.Success {
		t.Fatal("Response to valid set command after initial deletion does not have expected contents")
	}

	validateResponseToClient(t, KeyRequest{Directory: "/d", Key: "k", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/get_metadata", &mr, "", http.StatusOK)
	seq++
	if mr.Directory != "/d" || mr.Key != "k" || mr.IsDirectory {
		t.Fatal("Metadata response after initial deletion has incorrect directory, key, or is_directory value")
	}
	if mr.Version > kVersion {
		t.Fatal("Metadata response after initial deletion suggests it was not reset when deleted")
	}
	if mr.LeaderIdx != leader {
		t.Fatal("Metadata response has incorrect leader index")
	}

	fmt.Println("ok")

	fmt.Print("Checking that directories are properly deleted along with their metadata ... ")

	dir := "/d"
	levels := 3
	for l := range levels {
		// all of these should return "success: true" and status code 201 Created
		validateResponseToClient(t, KeyRequest{Directory: dir, Key: "subdir" + strconv.Itoa(l), SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/create", &ksr, "", http.StatusCreated)
		seq++
		if ksr.Directory != dir || ksr.Key != "subdir"+strconv.Itoa(l) || !ksr.Success {
			t.Fatal("Response to valid create command does not have expected contents")
		}
		dir += "/subdir" + strconv.Itoa(l)
	}

	// delete request on directory /d/subdir0 should succeed with return code 200 OK
	validateResponseToClient(t, KeyRequest{Directory: "/d", Key: "subdir0", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/delete", &ksr, "", http.StatusOK)
	seq++
	if ksr.Directory != "/d" || ksr.Key != "subdir0" || !ksr.Success {
		t.Fatal("Response to valid delete directory command does not have expected contents")
	}

	lr := ListResponse{}
	validateResponseToClient(t, DirectoryRequest{Directory: "/d", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/list", &lr, "", http.StatusOK)
	seq++
	if slices.Contains(lr.List, "subdir0") {
		t.Fatal("List response includes previously deleted directory")
	}

	// delete request on directory /d should succeed with return code 200 OK
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "d", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/delete", &ksr, "", http.StatusOK)
	seq++
	if ksr.Directory != "/" || ksr.Key != "d" || !ksr.Success {
		t.Fatal("Response to valid delete directory command does not have expected contents")
	}

	rc, er = validateResponseToClient(t, KeyRequest{Directory: "/d", Key: "k", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/get", &kvm, "", 0)
	seq++
	if er == nil || rc == http.StatusOK {
		t.Fatal("Get endpoint accepted request for deleted key without error")
	}

	rc, er = validateResponseToClient(t, KeyRequest{Directory: "/d", Key: "k", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/get_metadata", &mr, "", 0)
	seq++
	if er == nil || rc == http.StatusOK {
		t.Fatal("Get-metadata endpoint accepted request for deleted key without error")
	}

	rc, er = validateResponseToClient(t, KeyRequest{Directory: "/", Key: "d", SeqNumber: seq, ClientID: clientName}, leaderClientPort, "/get_metadata", &mr, "", 0)
	seq++
	if er == nil || rc == http.StatusOK {
		t.Fatal("Get-metadata endpoint accepted request for deleted directory without error")
	}

	fmt.Println("ok")
}

// test that each cluster participant can support multiple independent Raft peers in different
// groups and that the various Raft peers are all actually used.
func TestFinal_MultipleGroups(t *testing.T) {
	clusterSize := 5
	addlGroups := 2
	addlGroupSize := 3

	ctrl := NewHKVCController(t, clusterSize, addlGroups, addlGroupSize)

	leaders := make(map[int]int)
	for g, _ := range ctrl.raftGroups {
		leaders[g], _ = ctrl.getGroupLeaderCommit(g)
		if leaders[g] < 0 {
			t.Fatalf("Raft group %d was created but does not have a leader", g)
		}
	}

	groupClientAddrs := make(map[int][]string)
	groupClientPorts := make(map[int][]int) // [0] has client ports for all participants, others are subsets
	for g, ids := range ctrl.raftGroups {
		for _, pidx := range ids {
			groupClientAddrs[g] = append(groupClientAddrs[g], ctrl.setupInfo[pidx].ClientAddr)
			port, e := strconv.Atoi(strings.Split(ctrl.setupInfo[pidx].ClientAddr, ":")[1])
			if e != nil {
				t.Fatal("Error extracting port number from string: ", e)
			}
			groupClientPorts[g] = append(groupClientPorts[g], port)
		}
	}

	clientName := randSeq(12)
	mr := MetadataResponse{}
	ksr := KeySuccessResponse{}

	seq := 0

	fmt.Print("Checking that everyone agrees on management of root directory by group 0 ... ")

	for i := range clusterSize {
		// get_metadata request for root directory
		validateResponseToClient(t, KeyRequest{Directory: "/", Key: ".", SeqNumber: seq, ClientID: clientName}, groupClientPorts[0][i], "/get_metadata", &mr, "", http.StatusOK)
		seq++
		if mr.Directory != "/" || mr.Key != "." || !mr.IsDirectory {
			t.Fatal("Metadata response has incorrect directory, key, or is_directory value")
		}
		// check that every group member is included in the group managing root directory
		if !sameStringSlices(mr.PAddrList, groupClientAddrs[0]) {
			t.Fatalf("Metadata from participant %d reports incorrect participants in group 0", i)
		}
		// check that everyone agrees on the leader of group 0 (by address, not index)
		if mr.PAddrList[mr.LeaderIdx] != groupClientAddrs[0][leaders[0]] {
			t.Fatalf("Metadata from participant %d reports incorrect leader in group 0", i)
		}
	}
	fmt.Println("ok")

	fmt.Print("Checking that keys created in the same directory are all managed by the same group ... ")
	keysPer := 8

	// create a new directory in root (root should be managed by group 0)
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: seq, ClientID: clientName}, groupClientPorts[0][leaders[0]], "/create", &ksr, "", http.StatusCreated)
	seq++

	// identify the leader of the group managing the new directory (as index in groupClientPorts[0], since group# is unknown)
	dirLeader := findDirLeaderViaList(t, DirectoryRequest{Directory: "/dir", SeqNumber: seq, ClientID: clientName}, groupClientPorts[0])
	seq++
	if dirLeader == -1 {
		t.Fatal("Leader not found after new directory created")
	}

	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: seq, ClientID: clientName}, groupClientPorts[0][dirLeader], "/get_metadata", &mr, "", http.StatusOK)
	seq++
	if mr.Directory != "/" || mr.Key != "dir" || !mr.IsDirectory {
		t.Fatal("Metadata response has incorrect directory, key, or is_directory value")
	}

	// find group(s) matching groupClientAddrs for mr.PAddrList in the response
	dirGroup := -1
	for g, _ := range ctrl.raftGroups {
		if sameStringSlices(mr.PAddrList, groupClientAddrs[g]) {
			if slices.Contains(groupClientPorts[g], groupClientPorts[0][dirLeader]) {
				if dirLeader == leaders[g] {
					// group found, though it's possible there are multiple groups with the same participants and leader...
					dirGroup = g
				}
			}
		}
	}
	if dirGroup == -1 {
		t.Fatal("New directory leader not found, possibly due to inconsistent state")
	}

	for k := range keysPer {
		validateResponseToClient(t, KeyValueMessage{Directory: "/dir", Key: "k" + strconv.Itoa(k), Value: "v" + strconv.Itoa(k), SeqNumber: seq, ClientID: clientName}, groupClientPorts[0][leaders[dirGroup]], "/set", &ksr, "", http.StatusCreated)
		seq++
		if ksr.Directory != "/dir" || ksr.Key != "k"+strconv.Itoa(k) || !ksr.Success {
			t.Fatal("Response to valid set command does not have expected contents")
		}
	}

	// `rm -rf /dir`
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: seq, ClientID: clientName}, groupClientPorts[0][leaders[0]], "/delete", &ksr, "", http.StatusOK)
	seq++
	fmt.Println("ok")

	fmt.Print("Verifying that all created Raft groups are managing stored content ... ")

	groupHits := make(map[int]int)
	for g, _ := range ctrl.raftGroups {
		groupHits[g] = 0
	}
	dir := "/"
	fanout := 4
	keysPer = 2
	for f0 := range fanout {
		validateResponseToClient(t, KeyRequest{Directory: dir, Key: "subdir0" + strconv.Itoa(f0), SeqNumber: seq, ClientID: clientName}, groupClientPorts[0][leaders[0]], "/create", &ksr, "", http.StatusCreated)
		seq++
		if ksr.Directory != dir || ksr.Key != "subdir0"+strconv.Itoa(f0) || !ksr.Success {
			t.Fatal("Response to valid create command does not have expected contents")
		}

		dirLeader0 := findDirLeaderViaList(t, DirectoryRequest{Directory: dir + "subdir0" + strconv.Itoa(f0), SeqNumber: seq, ClientID: clientName}, groupClientPorts[0])
		seq++
		if dirLeader0 == -1 {
			t.Fatal("Leader not found after creating new directory " + dir + "subdir0" + strconv.Itoa(f0))
		}

		validateResponseToClient(t, KeyRequest{Directory: dir, Key: "subdir0" + strconv.Itoa(f0), SeqNumber: seq, ClientID: clientName}, groupClientPorts[0][dirLeader0], "/get_metadata", &mr, "", http.StatusOK)
		seq++
		if mr.Directory != dir || mr.Key != "subdir0"+strconv.Itoa(f0) || !mr.IsDirectory {
			t.Fatal("Metadata response has incorrect directory, key, or is_directory value")
		}

		dirGroup0 := -1
		for g, _ := range ctrl.raftGroups {
			if sameStringSlices(mr.PAddrList, groupClientAddrs[g]) {
				if slices.Contains(groupClientPorts[g], groupClientPorts[0][dirLeader0]) {
					if dirLeader0 == leaders[g] {
						groupHits[g] += 1
						dirGroup0 = g
					}
				}
			}
		}
		if dirGroup0 == -1 {
			t.Fatal("New directory leader not found, possibly due to inconsistent state")
		}

		dir += "subdir0" + strconv.Itoa(f0)

		for k := range keysPer {
			validateResponseToClient(t, KeyValueMessage{Directory: dir, Key: "k" + strconv.Itoa(k), Value: "v" + strconv.Itoa(k), SeqNumber: seq, ClientID: clientName}, groupClientPorts[0][leaders[dirGroup0]], "/set", &ksr, "", http.StatusCreated)
			seq++
			if ksr.Directory != dir || ksr.Key != "k"+strconv.Itoa(k) || !ksr.Success {
				t.Fatal("Response to valid set command does not have expected contents")
			}
		}

		for f1 := range fanout {
			validateResponseToClient(t, KeyRequest{Directory: dir, Key: "subdir1" + strconv.Itoa(f1), SeqNumber: seq, ClientID: clientName}, groupClientPorts[0][leaders[dirGroup0]], "/create", &ksr, "", http.StatusCreated)
			seq++
			if ksr.Directory != dir || ksr.Key != "subdir1"+strconv.Itoa(f1) || !ksr.Success {
				t.Fatal("Response to valid create command does not have expected contents")
			}

			dirLeader1 := findDirLeaderViaList(t, DirectoryRequest{Directory: dir + "/subdir1" + strconv.Itoa(f1), SeqNumber: seq, ClientID: clientName}, groupClientPorts[0])
			seq++
			if dirLeader1 == -1 {
				t.Fatal("Leader not found after creating new directory " + dir + "/subdir1" + strconv.Itoa(f1))
			}

			validateResponseToClient(t, KeyRequest{Directory: dir, Key: "subdir1" + strconv.Itoa(f1), SeqNumber: seq, ClientID: clientName}, groupClientPorts[0][dirLeader1], "/get_metadata", &mr, "", http.StatusOK)
			seq++
			if mr.Directory != dir || mr.Key != "subdir1"+strconv.Itoa(f1) || !mr.IsDirectory {
				t.Fatal("Metadata response has incorrect directory, key, or is_directory value")
			}

			dirGroup1 := -1
			for g, _ := range ctrl.raftGroups {
				if sameStringSlices(mr.PAddrList, groupClientAddrs[g]) {
					if slices.Contains(groupClientPorts[g], groupClientPorts[0][dirLeader1]) {
						if dirLeader1 == leaders[g] {
							groupHits[g] += 1
							dirGroup1 = g
						}
					}
				}
			}
			if dirGroup1 == -1 {
				t.Fatal("New directory leader not found, possibly due to inconsistent state")
			}

			dir += "/subdir1" + strconv.Itoa(f1)
			for k := range keysPer {
				validateResponseToClient(t, KeyValueMessage{Directory: dir, Key: "k" + strconv.Itoa(k), Value: "v" + strconv.Itoa(k), SeqNumber: seq, ClientID: clientName}, groupClientPorts[0][leaders[dirGroup1]], "/set", &ksr, "", http.StatusCreated)
				seq++
				if ksr.Directory != dir || ksr.Key != "k"+strconv.Itoa(k) || !ksr.Success {
					t.Fatal("Response to valid set command does not have expected contents")
				}
			}

			for f2 := range fanout {
				validateResponseToClient(t, KeyRequest{Directory: dir, Key: "subdir2" + strconv.Itoa(f2), SeqNumber: seq, ClientID: clientName}, groupClientPorts[0][leaders[dirGroup1]], "/create", &ksr, "", http.StatusCreated)
				seq++
				if ksr.Directory != dir || ksr.Key != "subdir2"+strconv.Itoa(f2) || !ksr.Success {
					t.Fatal("Response to valid create command does not have expected contents")
				}

				dirLeader2 := findDirLeaderViaList(t, DirectoryRequest{Directory: dir + "/subdir2" + strconv.Itoa(f2), SeqNumber: seq, ClientID: clientName}, groupClientPorts[0])
				seq++
				if dirLeader2 == -1 {
					t.Fatal("Leader not found after creating new directory " + dir + "/subdir2" + strconv.Itoa(f2))
				}

				validateResponseToClient(t, KeyRequest{Directory: dir, Key: "subdir2" + strconv.Itoa(f2), SeqNumber: seq, ClientID: clientName}, groupClientPorts[0][dirLeader2], "/get_metadata", &mr, "", http.StatusOK)
				seq++
				if mr.Directory != dir || mr.Key != "subdir2"+strconv.Itoa(f2) || !mr.IsDirectory {
					t.Fatal("Metadata response has incorrect directory, key, or is_directory value")
				}

				dirGroup2 := -1
				for g, _ := range ctrl.raftGroups {
					if sameStringSlices(mr.PAddrList, groupClientAddrs[g]) {
						if slices.Contains(groupClientPorts[g], groupClientPorts[0][dirLeader2]) {
							if dirLeader2 == leaders[g] {
								groupHits[g] += 1
								dirGroup2 = g
							}
						}
					}
				}
				if dirGroup2 == -1 {
					t.Fatal("New directory leader not found, possibly due to inconsistent state")
				}

				dir += "/subdir2" + strconv.Itoa(f2)
				for k := range keysPer {
					validateResponseToClient(t, KeyValueMessage{Directory: dir, Key: "k" + strconv.Itoa(k), Value: "v" + strconv.Itoa(k), SeqNumber: seq, ClientID: clientName}, groupClientPorts[0][leaders[dirGroup2]], "/set", &ksr, "", http.StatusCreated)
					seq++
					if ksr.Directory != dir || ksr.Key != "k"+strconv.Itoa(k) || !ksr.Success {
						t.Fatal("Response to valid set command does not have expected contents")
					}
				}
				dir = dir[:strings.LastIndexByte(dir, '/')]
			}
			dir = dir[:strings.LastIndexByte(dir, '/')]
		}
		dir = "/"
	}

	for g, _ := range ctrl.raftGroups {
		if groupHits[g] < fanout {
			t.Fatalf("Assignment of work to Raft group %d is insufficient", g)
		}
	}

	fmt.Println("ok")
}

package hkvc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand"
	"net/http"
	"remote" // please do not change this
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

const WaitForRaft time.Duration = 2 * time.Second

//
// Utility functions used by tests
//

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// generate a random sequence of n letters
func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// utility function for randomly choosing k values from {0, ... , N-1}
func randomSubset(N, k int) []int {
	var i int = 0
	if k == N { // special case where the subset is the entire set
		s := make([]int, k)
		for i := range k {
			s[i] = i
		}
		return s
	}

	// this could be optimized by treating k > N/2 separately, but ...
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	m := make(map[int]bool)
	for i < k {
		r := rng.Intn(N)
		if !m[r] {
			m[r] = true
			i++
		}
	}
	return slices.Collect(maps.Keys(m))
}

// helper utility for checking whether given []string are the same, though
// not necessarily in the same order
func sameStringSlices(one, two []string) bool {
	if len(one) != len(two) {
		return false
	}
	for _, s := range one {
		if !slices.Contains(two, s) {
			return false
		}
	}
	return true
}

// helper utility for POSTing a client message to an http endpoint and
// populating the expected response type or returning an error response.
// `endpoint` arg is expected to include the leading `/` character.
// return standard error type if intermediate steps fail.
func getResponse(port int, endpoint string, msg []byte, v any) (int, *HKVCErrorResponse, error) {
	url := "http://localhost:" + strconv.Itoa(port) + endpoint
	resp, eP := http.Post(url, "application/json", bytes.NewBuffer(msg))
	if eP != nil {
		return -1, nil, errors.New("Error sending http request in getResponse:" + eP.Error())
	}

	code := resp.StatusCode
	byts, eI := io.ReadAll(resp.Body)
	if eI != nil {
		return -1, nil, errors.New("Error decoding " + endpoint + " response in getResponse:" + eI.Error())
	}
	resp.Body.Close()
	str := string(byts)

	d := json.NewDecoder(strings.NewReader(str))
	d.DisallowUnknownFields()

	if strings.Contains(str, "error_type") {
		e := &HKVCErrorResponse{}
		if eED := d.Decode(e); eED != nil {
			return code, nil, errors.New("Error decoding " + endpoint + " error response in getResponse:" + eED.Error())
		}
		return code, e, nil
	}
	if eSD := d.Decode(v); eSD != nil {
		return code, nil, errors.New("Error decoding " + endpoint + " success response in getResponse:" + eSD.Error())
	}
	return code, nil, nil
}

// helper utility to validate the expected response to a client message, failing
// the containing test if the response does not match the expected behavior.
// arguments:
// -- msg: a populated struct containing message fields the client is submitting
// -- port: client request sent to localhost:port
// -- endoint: api endpoint (with "/") to direct client request
// -- resp: pointer to placeholder for response message
// -- expError: expected error type or "" if no error should occur
// -- expStatusCode: expected HTTP status code number
// return values:
// -- rc: http status code returned with response
// -- er: pointer to HKVCErrorResponse message returned as response
func validateResponseToClient(t *testing.T, msg any, port int, endpoint string, resp any, expError string, expStatusCode int) (int, *HKVCErrorResponse) {
	jsonmsg, e := json.Marshal(msg)
	if e != nil {
		t.Fatalf("Error encoding %T: %s", msg, e)
	}
	rc, er, err := getResponse(port, endpoint, jsonmsg, resp)
	if err != nil {
		t.Fatal(err)
	}
	if expStatusCode == http.StatusOK || expStatusCode == http.StatusCreated {
		if er != nil || rc != expStatusCode {
			t.Fatalf("Endpoint %s returned error on valid %T", endpoint, msg)
		}
	} else if expStatusCode > 0 {
		if er == nil || (expError != "" && er.ErrorType != expError) || rc != expStatusCode {
			t.Fatalf("Endpoint %s accepted or reported incorrect error type/code on invalid %T", endpoint, msg)
		}
	}
	return rc, er
}

//
// HKVC data structures, builder, and methods used by tests
//

type HKVCController struct {
	t           *testing.T              // allow HKVCController to affect test
	clusterSize int                     // total #participants in HKVC cluster
	nAddlGroups int                     // number of Raft groups beyond the base group (w/ everyone)
	groupSize   int                     // #participants in each addl Raft group
	basePort    int                     // starting CalleeStub port number, +1 for each subsequent interface
	stubs       []*HKVCControlInterface // stubs for communicating with cluster participants
	raftGroups  map[int][]int           // static collection of raft groups, created by HKVCController
	setupInfo   []HKVCSetupInfo         // id and port information sent to all participants
}

// create a HKVCController to facilitate tests
//
//	-- clSize: number of cluster participants
//	-- addlGroups: number of Raft groups beyond base "everyone" group
//	-- grSize: size of all additional groups
func NewHKVCController(t *testing.T, clSize, addlGroups, grSize int) *HKVCController {
	ctrl := &HKVCController{}
	ctrl.t = t
	ctrl.clusterSize = clSize
	ctrl.nAddlGroups = addlGroups
	ctrl.groupSize = grSize
	ctrl.stubs = make([]*HKVCControlInterface, clSize)
	ctrl.basePort = 20000 + rand.Intn(10000)
	ctrl.setupInfo = make([]HKVCSetupInfo, clSize)

	// create base group of sizes clSize, plus addlGroups each of size grSize
	ctrl.raftGroups = make(map[int][]int)
	groupIDs := randomSubset(254, addlGroups)         // additional groupIDs will be in [1, 255], because +1 below
	ctrl.raftGroups[0] = randomSubset(clSize, clSize) // everyone is in the base group with groupID = 0
	for i := range addlGroups {
		ctrl.raftGroups[groupIDs[i]+1] = randomSubset(clSize, grSize) // raft group managing a shard
	}
	// generate unique IDs and sequential addresses for all Raft peers
	ids := randomSubset(65000, clSize)
	for i := range clSize {
		raftGroupAddrs := make(map[int]string)
		raftGroupAddrs[0] = "localhost:" + strconv.Itoa(ctrl.basePort+(3+addlGroups)*i+2)
		for j := range addlGroups {
			raftGroupAddrs[groupIDs[j]+1] = "localhost:" + strconv.Itoa(ctrl.basePort+(3+addlGroups)*i+3+j)
		}
		ctrl.setupInfo[i] = HKVCSetupInfo{
			Id:          ids[i],
			ClientAddr:  "localhost:" + strconv.Itoa(ctrl.basePort+(3+addlGroups)*i),
			ControlAddr: "localhost:" + strconv.Itoa(ctrl.basePort+(3+addlGroups)*i+1),
			RaftAddrs:   raftGroupAddrs,
		}
	}

	for i := range clSize {
		go NewHKVCParticipant(ctrl.setupInfo, i, ctrl.raftGroups)
		ctrl.stubs[i] = &HKVCControlInterface{}
		err := remote.CallerStubCreator(ctrl.stubs[i], ctrl.setupInfo[i].ControlAddr, false, false)
		if err != nil {
			ctrl.t.Fatalf("Cannot create HKVCController stub for HKVC participant: %s", err.Error())
		}
	}

	// register a cleanup function to tell all participants to shut down
	t.Cleanup(func() {
		for i := range ctrl.clusterSize {
			ctrl.stubs[i].Terminate()
		}
	})

	// wait to spin up all participants, then activate them all
	time.Sleep(2 * time.Second)
	for i := range ctrl.clusterSize {
		ctrl.connect(i)
	}
	// wait again for Raft to stabilize
	time.Sleep(WaitForRaft)

	return ctrl
}

// (re)connect a cluster participant by telling it to (re)start its Raft and client interfaces
func (ctrl *HKVCController) connect(i int) {
	for {
		re := ctrl.stubs[i].Activate()
		if re.Error() == "" {
			break
		}
	}
}

// disconnect a cluster participant by telling it to stop its Raft and client interfaces
func (ctrl *HKVCController) disconnect(i int) {
	for {
		re := ctrl.stubs[i].Deactivate()
		if re.Error() == "" {
			break
		}
	}
}

// get the current leader and commit index of raft group identified by groupID parameter, if there is a leader
func (ctrl *HKVCController) getGroupLeaderCommit(groupID int) (int, int) {
	group, exists := ctrl.raftGroups[groupID]
	if !exists {
		return -1, -1
	}

	for _, idx := range group {
		sr, re := ctrl.stubs[idx].GetStatus()
		if re.Error() != "" {
			fmt.Println("warning: remote call GetStatus failed -- " + re.Error())
			continue
		}
		if sr.Active && sr.GroupLeader[groupID] {
			if commit, exists := sr.GroupCommit[groupID]; exists {
				return idx, commit
			} else {
				return idx, -1
			}
		}
	}
	return -1, -1
}

//
// Checkpoint tests start here !!!
//

// test handling of invalid messages to client interface
func TestCheckpoint_ClientArgs(t *testing.T) {
	clusterSize := 1
	ctrl := NewHKVCController(t, clusterSize, 0, 0)

	clientName := randSeq(12)

	fmt.Print("Checking error responses by list endpoint ... ")

	validateResponseToClient(t, DirectoryRequest{Directory: "", SeqNumber: 0, ClientID: clientName}, ctrl.basePort, "/list", &ListResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, DirectoryRequest{Directory: "dir", SeqNumber: 1, ClientID: clientName}, ctrl.basePort, "/list", &ListResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, DirectoryRequest{Directory: "/dir:name", SeqNumber: 2, ClientID: clientName}, ctrl.basePort, "/list", &ListResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, DirectoryRequest{Directory: "/dir", SeqNumber: 3, ClientID: clientName}, ctrl.basePort, "/list", &ListResponse{}, DirNotFoundError, http.StatusNotFound)

	fmt.Println("ok")

	fmt.Print("Checking error responses by get_metadata endpoint ... ")

	validateResponseToClient(t, KeyRequest{Directory: "", Key: "key", SeqNumber: 4, ClientID: clientName}, ctrl.basePort, "/get_metadata", &MetadataResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyRequest{Directory: "/dir", Key: "", SeqNumber: 5, ClientID: clientName}, ctrl.basePort, "/get_metadata", &MetadataResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyRequest{Directory: "dir", Key: "key", SeqNumber: 6, ClientID: clientName}, ctrl.basePort, "/get_metadata", &MetadataResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyRequest{Directory: "/dir:name", Key: "key", SeqNumber: 7, ClientID: clientName}, ctrl.basePort, "/get_metadata", &MetadataResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyRequest{Directory: "/dir", Key: "key", SeqNumber: 8, ClientID: clientName}, ctrl.basePort, "/get_metadata", &MetadataResponse{}, DirNotFoundError, http.StatusNotFound)

	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "key", SeqNumber: 9, ClientID: clientName}, ctrl.basePort, "/get_metadata", &MetadataResponse{}, KeyNotFoundError, http.StatusNotFound)

	fmt.Println("ok")

	fmt.Print("Checking error responses by get endpoint ... ")

	validateResponseToClient(t, KeyRequest{Directory: "", Key: "key", SeqNumber: 10, ClientID: clientName}, ctrl.basePort, "/get", &KeyValueMessage{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyRequest{Directory: "dir", Key: "key", SeqNumber: 11, ClientID: clientName}, ctrl.basePort, "/get", &KeyValueMessage{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyRequest{Directory: "/dir:name", Key: "key", SeqNumber: 12, ClientID: clientName}, ctrl.basePort, "/get", &KeyValueMessage{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyRequest{Directory: "/dir", Key: "key", SeqNumber: 13, ClientID: clientName}, ctrl.basePort, "/get", &KeyValueMessage{}, DirNotFoundError, http.StatusNotFound)

	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "key", SeqNumber: 14, ClientID: clientName}, ctrl.basePort, "/get", &KeyValueMessage{}, KeyNotFoundError, http.StatusNotFound)

	fmt.Println("ok")

	fmt.Print("Checking error responses by set endpoint ... ")

	validateResponseToClient(t, KeyValueMessage{Directory: "", Key: "key", Value: "abcd", SeqNumber: 15, ClientID: clientName}, ctrl.basePort, "/set", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyValueMessage{Directory: "dir", Key: "key", Value: "abcd", SeqNumber: 16, ClientID: clientName}, ctrl.basePort, "/set", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyValueMessage{Directory: "/dir:name", Key: "key", Value: "abcd", SeqNumber: 17, ClientID: clientName}, ctrl.basePort, "/set", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyValueMessage{Directory: "/dir", Key: "key", Value: "abcd", SeqNumber: 18, ClientID: clientName}, ctrl.basePort, "/set", &KeySuccessResponse{}, DirNotFoundError, http.StatusNotFound)

	fmt.Println("ok")

	fmt.Print("Checking error responses by create endpoint ... ")

	validateResponseToClient(t, KeyRequest{Directory: "", Key: "key", SeqNumber: 19, ClientID: clientName}, ctrl.basePort, "/create", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyRequest{Directory: "dir", Key: "key", SeqNumber: 20, ClientID: clientName}, ctrl.basePort, "/create", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyRequest{Directory: "/dir:name", Key: "key", SeqNumber: 21, ClientID: clientName}, ctrl.basePort, "/create", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyRequest{Directory: "/dir", Key: "key", SeqNumber: 22, ClientID: clientName}, ctrl.basePort, "/create", &KeySuccessResponse{}, DirNotFoundError, http.StatusNotFound)

	fmt.Println("ok")

	fmt.Print("Checking error responses by delete endpoint ... ")

	validateResponseToClient(t, KeyRequest{Directory: "", Key: "key", SeqNumber: 23, ClientID: clientName}, ctrl.basePort, "/delete", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyRequest{Directory: "dir", Key: "key", SeqNumber: 24, ClientID: clientName}, ctrl.basePort, "/delete", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyRequest{Directory: "/dir:name", Key: "key", SeqNumber: 25, ClientID: clientName}, ctrl.basePort, "/delete", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyRequest{Directory: "/dir", Key: "", SeqNumber: 26, ClientID: clientName}, ctrl.basePort, "/delete", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)

	validateResponseToClient(t, KeyRequest{Directory: "/dir", Key: "key", SeqNumber: 27, ClientID: clientName}, ctrl.basePort, "/delete", &KeySuccessResponse{}, DirNotFoundError, http.StatusNotFound)

	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "key", SeqNumber: 28, ClientID: clientName}, ctrl.basePort, "/delete", &KeySuccessResponse{}, KeyNotFoundError, http.StatusNotFound)

	fmt.Println("ok")
}

// test setup of cluster nodes and leader response to client
func TestCheckpoint_Initialize(t *testing.T) {
	clusterSize := 3

	fmt.Print("Checking controller creation ... ")
	ctrl := NewHKVCController(t, clusterSize, 0, 0)
	fmt.Println("ok")

	clientName := randSeq(12)

	fmt.Print("Checking controller can get participant status ... ")
	_, re := ctrl.stubs[0].GetStatus()
	if re.Error() != "" {
		t.Fatalf("remote error getting status from HKVC participant: %s", re.Error())
	}
	fmt.Println("ok")

	fmt.Print("Checking client can query cluster leader ... ")
	leader, _ := ctrl.getGroupLeaderCommit(0)

	validateResponseToClient(t, DirectoryRequest{Directory: "/", SeqNumber: 0, ClientID: clientName}, ctrl.basePort+(3+ctrl.nAddlGroups)*leader, "/list", &ListResponse{}, "", http.StatusOK)

	fmt.Println("ok")
}

// test that non-leader rejects all relevant commands with appropriate errors
func TestCheckpoint_NonLeaderRejects(t *testing.T) {
	clusterSize := 3
	ctrl := NewHKVCController(t, clusterSize, 0, 0)
	leader, _ := ctrl.getGroupLeaderCommit(0)
	nonLeader := (leader + 1) % clusterSize
	nonLeaderClientPort := ctrl.basePort + (3+ctrl.nAddlGroups)*nonLeader

	clientName := randSeq(12)

	fmt.Print("Checking non-leader rejects commands with appropriate errors ... ")

	validateResponseToClient(t, DirectoryRequest{Directory: "/", SeqNumber: 0, ClientID: clientName}, nonLeaderClientPort, "/list", &ListResponse{}, NonLeaderError, http.StatusForbidden)

	validateResponseToClient(t, KeyValueMessage{Directory: "/", Key: "key", Value: "value", SeqNumber: 1, ClientID: clientName}, nonLeaderClientPort, "/set", &KeySuccessResponse{}, NonLeaderError, http.StatusForbidden)

	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "key", SeqNumber: 2, ClientID: clientName}, nonLeaderClientPort, "/get", &KeyValueMessage{}, NonLeaderError, http.StatusForbidden)

	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "key", SeqNumber: 3, ClientID: clientName}, nonLeaderClientPort, "/create", &KeySuccessResponse{}, NonLeaderError, http.StatusForbidden)

	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "key", SeqNumber: 4, ClientID: clientName}, nonLeaderClientPort, "/delete", &KeySuccessResponse{}, NonLeaderError, http.StatusForbidden)

	fmt.Println("ok")
}

// test key-value store creation using set and then validate using list and get
func TestCheckpoint_SetListGetKVStore(t *testing.T) {
	clusterSize := 3
	ctrl := NewHKVCController(t, clusterSize, 0, 0)
	leader, _ := ctrl.getGroupLeaderCommit(0)
	leaderClientPort := ctrl.basePort + (3+ctrl.nAddlGroups)*leader

	clientName := randSeq(12)

	fmt.Print("Checking client use of set command to populate a small kv store ... ")
	ksr := KeySuccessResponse{}
	numKeys := 10
	keyList := make([]string, numKeys)
	valueList := make([]string, numKeys)
	for k := range numKeys {
		kvm := KeyValueMessage{
			Directory: "/",
			Key:       "key" + strconv.Itoa(k),
			Value:     "value #" + strconv.Itoa(k),
			SeqNumber: k,
			ClientID:  clientName,
		}
		keyList[k] = kvm.Key
		valueList[k] = kvm.Value

		validateResponseToClient(t, kvm, leaderClientPort, "/set", &ksr, "", http.StatusCreated)
	}
	fmt.Println("ok")

	fmt.Print("Checking client can correctly list the created keys ... ")
	lr := ListResponse{}

	validateResponseToClient(t, DirectoryRequest{Directory: "/", SeqNumber: numKeys, ClientID: clientName}, leaderClientPort, "/list", &lr, "", http.StatusOK)

	if !sameStringSlices(lr.List, keyList) || lr.Directory != "/" {
		t.Fatal("List response includes incorrect directory or list of keys")
	}
	fmt.Println("ok")

	fmt.Print("Checking values fetched using get match original values used in set ... ")
	kvm := KeyValueMessage{}
	for k := range numKeys {
		validateResponseToClient(t, KeyRequest{Directory: "/", Key: keyList[k], SeqNumber: numKeys + 1 + k, ClientID: clientName}, leaderClientPort, "/get", &kvm, "", http.StatusOK)
		if kvm.Value != valueList[k] {
			t.Fatalf("get response for k=%d does not contain value in set request", k)
		}
	}
	fmt.Println("ok")
}

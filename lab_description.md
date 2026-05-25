Lab 3 Description
Lab Summary
In this lab, you will build a hierarchical storage system that is a hybrid between a distributed hash table (DHT) and a distributed file system (DFS).  We'll refer to this design as a hierarchical key-value cluster (HKVC), as it includes a hierarchical directory structure similar to a DFS, but each directory has the possibility of housing an independent key-value store that is jointly managed by a group of cluster participants using Raft.  In the most general case, each HKVC participant can have an arbitrary number of Raft instances running, with each Raft instance helping to manage a subset of the key-value stores within the cluster.   In addition, each cluster participant exposes an HTTP-based client interface that multiplexes several API endpoints onto an HTTP server exposed on a single server port.  When a client wants to perform an action on the key-value cluster that involves a particular key-value store, it must interact with the participant containing the Raft leader in the group managing that particular key-value store.

 

Logistics
You should ideally continue to work with the same lab partner, if any, as in Lab 2.  The starter code and test code can be cloned into your own private repository from our Github Classroom via the link shared on the Canvas assignment.  You should use this repository to share and maintain your code with your partner as well as to allow the course staff to help when you are running into issues with your implementation.  The process for submission of this lab is nearly the same as previous labs and is detailed in a separate section later in this document.

As always, you are expected to submit only code that was typed by your own hands or those of your partner, not anything taken from anywhere online.  You are certainly welcome to consult tutorials and references for Go in general, but no copying code.

 

Checkpoint and Final Submissions
We will have two deadlines for Lab 3, and you will make two separate submissions to the testing system and to Canvas.  Both of these submissions will be graded on their corresponding components according to the rubric provided below.

Checkpoint: This initial submission will show that you are on track to complete the lab by the final deadline.  At this point, you should have a partially implemented client interface that minimally interacts with a Raft instance and a collection of in-memory data structures that house the stored key-value data and the storage hierarchy, namely the tests that include "Checkpoint" in the name.  Built/rendered documentation is not required for the Checkpoint submission, but the auto-grader should be able to run your code and determine that the checkpoint tests succeed. When you submit your code for testing, you will be able to see which tests pass and how many points the auto-grader has assigned to your submission.  We do not have any hidden tests.

Final: The final submission should include your complete HKVC implementation that passes all tests (both "Checkpoint" and "Final") along with other required artifacts, including the following:

Completed, well-commented/documented code for your HKVC implementation, with built/rendered documentation output (using make docs or similar).
A brief commentary about any potential failure scenarios that you have identified that are not checked by the test code or any limitations on how your HKVC implementation could be used.
Optionally, any changes that you would recommend for this lab or improvements you think should be made.
The total grade, out of 250 points, will be allocated according to the break-down in Table 1.

Table 1: break-down of points by task for Lab 3
Task	Points
Points for passing each test	
        TestCheckpoint_ClientArgs	10
        TestCheckpoint_Initialize	10
        TestCheckpoint_NonLeaderRejects	5
        TestCheckpoint_SetListGetKVStore	35
        TestFinal_ClientSequencing	20
        TestFinal_CreateHierarchy	20
        TestFinal_RespondAfterCommit	20
        TestFinal_FaultyLeaders	30
        TestFinal_DeleteAndRebuild	30
        TestFinal_MultipleGroups	40
Documentation (quality, completeness, etc.)	30
Total	250
While the use of the Raft protocol means that every run of the tests has non-deterministic factors, the tests are designed under the assumption that your Raft code reliably passed the tests from the previous lab. By doing this, your implementation for this lab should be able to pass the tests in a nearly deterministic manner, so each test is only run one time.

 

Building your HKVC
The overall design of your HKVC participant will involve the orchestration of interactions between the client interface, the underlying Raft instances, and the storage data structures.  In your design, the storage data structures are the dynamic state that is managed by Raft, so you will need to add the Apply functionality of the Raft protocol.  In addition, there are a few important details about interactions with clients in Section 8 of the extended Raft paper, so make sure to read that section if you didn't read it before.  Most importantly, when a client makes an appropriate API call, the HTTP interface must map that call into a new command that is put into the corresponding Raft leader's log, at which point your Raft implementation will do the work to commit the log entry, and only after it is committed is the HTTP interface allowed to respond to the client request.  As such, the bulk of the work to build the HKVC participant is in stitching together the Raft instance(s) with the client interface, which you will need to build.

 

Client Interface Specification
Your client interface must support a total of six (6) different API endpoints, which are named list, get_metadata, get, set, create, and delete.  Interactions between clients and API are through HTTP request/response messages that carry JSON-encoded messages.  Each API endpoint also has the ability to respond with different types of errors.  All of the message structures are included in the starter code and used by the test code.  The starter code repository also includes a very detailed specification of the client API endpoints, and the test code will perform extensive testing to ensure that the specification is followed by your implementation.  Substantial parts of the client API specification are independent of the Raft integration, and this is the focus of the early tests.

The test code provides a lot of additional details of how the test code spawns each HKVC participant and provides all of the information needed to create its interfaces.  Make sure to read all of the comment blocks in the starter code, and pay very close attention to the NewHKVCController function in the test code, which passes a lot of information to each new HKVC participant in a call to each NewHKVCParticipant, which works in a similar way to the NewRaftPeer in the previous lab.

Important notes:

Your implementation cannot use any form of communication between participants other than using the existing RPC and HTTP interfaces (i.e., no files, shared databases, channels, etc.).  Your HKVC implementation should be entirely capable of being launched with different participants on completely different machines interacting over a network.
When configuring the CalleeStub and caller stub components, you are encouraged to set both LeakySocket parameters to false.  Setting either or both of these to true will make it harder to pass the provided test cases.  You are certainly welcome to experiment with the performance of your implementation when the network degrades, but the test cases do not account for arbitrary LeakySocket parameters.
As mentioned, there are a lot of initial comments in the starter code.  These are intended to help you, so it is important that you read them early in the process.  You can also use the rules in the provided Makefile to generate the package documentation from these comments, as with previous labs.  You are also required to remove our provided comments and replace them with your own comments as part of your overall documentation of your implementation.
Any use of open-source code (including partial/incomplete/starter code from other Universities) is strictly forbidden.
 

Helpful Reminders
In general, the goal of this lab is to get experience incorporating our previously built tools and techniques into a fully functional distributed system that can be used by clients for a real purpose.  The goal isn't to make you struggle with programming, but rather to leverage advanced programming capabilities to build a system.  If there's ever anything you don't know how to do in Go, please ask!  Even if we don't know off hand, we'll help you work through the details.  Aside from programming itself, if there is any aspect of the system specification, API documentation, or expectations of the test code that you'd like to discuss, please ask!  Remember, the first step to designing a large application should never be writing code.  If anything else comes up, please ask!  Never hesitate to reach out to the course staff via Piazza, Discord, or office hours.  Helping you learn is our job.

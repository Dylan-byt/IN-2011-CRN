IN2011 Computer Networks Coursework - CRN Node Implementation

Name: Dylan Johnson
Email: dylan.johnson@city.ac.uk

==================
BUILD INSTRUCTIONS
==================


This project only uses available libraries not using any external dependancy. 

To compile the code, all .java files in the same folder and compile it in the folder

    javac *.java (command to compile the code)

This will compile the node and any and all other java files provided.

================
RUN INSTRUCTIONS
================

To run the Azure lab smoke test, use:

    java AzureLabTest dylan.johnson@city.ac.uk 10.216.34.177

If a specific port is needed, use:

    java AzureLabTest dylan.johnson@city.ac.uk 10.216.34.177 20110

The IP address should be changed if the Azure lab machine has a different 10.x.x.x address.

I also used the local test program during development:

    java LocalTest

This creates a local node using port 20110 and checks the main CRN features without using the Azure lab nodes.

To run this for the Azure Smoke Test, use dylan.johnson@city.ac.uk ip addr (dependant on the individual ip address)

to run the Local Test use,

    java LocalTest

I have also successfully run this with both Azure smoke test and the original Local Test along with others i used to confirm other functionality.

=======================
FUNCTIONALITY COMPLETED
=======================

My implementation completes the methods required by NodeInterface:

    setNodeName
    openPort
    handleIncomingMessages
    isActive
    pushRelay
    popRelay
    exists
    read
    write
    CAS

The node sends and receives CRN messages over UDP using the required format: a two-byte transaction ID, followed by a space, followed by the message body. Transaction IDs are matched correctly when responses are received, and requests are retransmitted if no response is received after about 5 seconds. Retransmission is limited to three retries.

The CRN string format has been implemented, including strings with spaces and empty strings.

The following message types are implemented:

    G/H   name request and response
    N/O   nearest request and response
    E/F   key existence request and response
    R/S   read request and response
    W/X   write request and response
    C/D   compare-and-swap request and response
    V     relay request
    I     information messages, which are safely ignored

The node stores its own address pair and can store other node address pairs. It supports passive mapping by asking unknown peers for their name when they contact the node, and active mapping through nearest-node requests. 

It limits stored address pairs to three per distance.

Data key/value functionality is implemented for D: keys. The node can write values, read them back, check whether they exist, and perform compare-and-swap updates.

CAS updates are handled atomically.

Relay support is also implemented. The node can use a relay stack, forward embedded messages through another node, rewrite the transaction ID for the forwarded message, and return the response to the original sender.

I added defensive handling for malformed and unknown messages so they dont crash the node. 

Duplicate requests with the same sender and transaction ID can also receive a cached response.

I tested the code using the AzureLabTest program on the Azure lab machine. The node successfully contacted the provided CRN test nodes, read the Jabberwocky entries, and wrote/read back a marker value. I also tested the implementation locally with LocalTest, which checked read, write, exists, CAS, relay, duplicate request handling, malformed message handling, transaction ID matching, and retransmission behaviour. 

=================
KNOWN LIMITATIONS
=================

- Very large or rapidly changing networks have not been fully tested.
- Relay support works for basic cases, but long relay chains have not been tested 
- Relay failure cases may not all be handled perfectly may encounter issues with some edge cases
- Duplicate request handling has not been tested much and may run into issues
- Azure subnet probing was added to help with lab test and is not a complete system.
- FeatureTest is not exhaustive and may not test every feature completely as it was made quickly.


==============================
WireShark pcapng file attached
==============================

The file along with the readme is the wire shark file with the evidence of the smoke test working with the provided code

use udp.port == 20110 as the filter the look at No. from 6181 to 7708 to view the entire poem 
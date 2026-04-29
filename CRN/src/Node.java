// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  YOUR_NAME_GOES_HERE
//  YOUR_STUDENT_ID_NUMBER_GOES_HERE
//  YOUR_EMAIL_GOES_HERE

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.*;

// DO NOT EDIT starts
// This gives the interface that your code must implement.
// These descriptions are intended to help you understand how the interface
// will be used. See the RFC for how the protocol works.

interface NodeInterface {

    /* These methods configure your node.
     * They must both be called once after the node has been created but
     * before it is used. */
    
    // Set the name of the node.
    public void setNodeName(String nodeName) throws Exception;

    // Open a UDP port for sending and receiving messages.
    public void openPort(int portNumber) throws Exception;


    /*
     * These methods query and change how the network is used.
     */

    // Handle all incoming messages.
    // If you wait for more than delay miliseconds and
    // there are no new incoming messages return.
    // If delay is zero then wait for an unlimited amount of time.
    public void handleIncomingMessages(int delay) throws Exception;
    
    // Determines if a node can be contacted and is responding correctly.
    // Handles any messages that have arrived.
    public boolean isActive(String nodeName) throws Exception;

    // You need to keep a stack of nodes that are used to relay messages.
    // The base of the stack is the first node to be used as a relay.
    // The first node must relay to the second node and so on.
    
    // Adds a node name to a stack of nodes used to relay all future messages.
    public void pushRelay(String nodeName) throws Exception;

    // Pops the top entry from the stack of nodes used for relaying.
    // No effect if the stack is empty
    public void popRelay() throws Exception;
    

    /*
     * These methods provide access to the basic functionality of
     * CRN-25 network.
     */

    // Checks if there is an entry in the network with the given key.
    // Handles any messages that have arrived.
    public boolean exists(String key) throws Exception;
    
    // Reads the entry stored in the network for key.
    // If there is a value, return it.
    // If there isn't a value, return null.
    // Handles any messages that have arrived.
    public String read(String key) throws Exception;

    // Sets key to be value.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean write(String key, String value) throws Exception;

    // If key is set to currentValue change it to newValue.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;

}
// DO NOT EDIT ends

public class Node implements NodeInterface {
    
    private String nodeName;
    private byte[] nodeHashID;
    private DatagramSocket socket;
    private int port;
    private Stack<String> relayStack;
    
    // Storage: key -> (nodeAddress, nodePort) as "address:port"
    private Map<String, String> addressPairs;
    // Storage: key -> value for data
    private Map<String, String> dataPairs;
    
    // Pending requests: txID -> (nodeName, age in ms, retryCount)
    private Map<Integer, PendingRequest> pendingRequests;
    private Map<String, PeerInfo> peerInfo;
    
    // Request/response tracking
    private static class PendingRequest {
        String targetNode;
        long createdTime;
        int retryCount;
        byte[] originalMessage;
        
        PendingRequest(String target, byte[] msg) {
            this.targetNode = target;
            this.originalMessage = msg;
            this.createdTime = System.currentTimeMillis();
            this.retryCount = 0;
        }
    }
    
    private static class PeerInfo {
        String address;
        int port;
        byte[] hashID;
        long lastSeen;
        
        PeerInfo(String addr, int p) throws Exception {
            this.address = addr;
            this.port = p;
            this.hashID = HashID.computeHashID(addr);
            this.lastSeen = System.currentTimeMillis();
        }
    }
    
    public Node() {
        relayStack = new Stack<>();
        addressPairs = new HashMap<>();
        dataPairs = new HashMap<>();
        pendingRequests = new HashMap<>();
        peerInfo = new HashMap<>();
    }
    
    // ===== Configuration Methods =====
    
    public void setNodeName(String name) throws Exception {
        this.nodeName = name;
        this.nodeHashID = HashID.computeHashID(name);
    }
    
    public void openPort(int portNumber) throws Exception {
        this.port = portNumber;
        this.socket = new DatagramSocket(portNumber);
        this.socket.setSoTimeout(100);
        // Store our own address
        addressPairs.put(nodeName, "127.0.0.1:" + port);
    }
    
    // ===== String Encoding/Decoding (CRN Format) =====
    
    private String encodeString(String s) {
        int spaceCount = 0;
        for (char c : s.toCharArray()) {
            if (c == ' ') spaceCount++;
        }
        return spaceCount + " " + s + " ";
    }
    
    private class DecodeResult {
        String value;
        int endPos;
        DecodeResult(String v, int pos) { this.value = v; this.endPos = pos; }
    }
    
    private DecodeResult decodeString(String msg, int startPos) throws Exception {
        int spaceIdx = msg.indexOf(' ', startPos);
        if (spaceIdx == -1) throw new Exception("Invalid string encoding");
        
        String countStr = msg.substring(startPos, spaceIdx);
        int spaceCount = Integer.parseInt(countStr);
        
        int endIdx = spaceIdx + 1;
        int foundSpaces = 0;
        while (endIdx < msg.length() && foundSpaces <= spaceCount) {
            if (msg.charAt(endIdx) == ' ') foundSpaces++;
            if (foundSpaces > spaceCount) break;
            endIdx++;
        }
        
        String value = msg.substring(spaceIdx + 1, endIdx - 1);
        return new DecodeResult(value, endIdx);
    }
    
    // ===== Distance Calculation =====
    
    private int calculateDistance(byte[] hash1, byte[] hash2) {
        int matchingBits = 0;
        for (int i = 0; i < hash1.length && i < hash2.length; i++) {
            byte xor = (byte)(hash1[i] ^ hash2[i]);
            for (int bit = 7; bit >= 0; bit--) {
                if (((xor >> bit) & 1) == 0) {
                    matchingBits++;
                } else {
                    return 256 - matchingBits;
                }
            }
        }
        return 256 - matchingBits;
    }
    
    // ===== Message Handling =====
    
    public void handleIncomingMessages(int delay) throws Exception {
        long startTime = System.currentTimeMillis();
        
        while (true) {
            long elapsed = System.currentTimeMillis() - startTime;
            if (delay > 0 && elapsed > delay) break;
            
            try {
                byte[] buffer = new byte[65536];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                
                byte[] txID = new byte[2];
                txID[0] = buffer[0];
                txID[1] = buffer[1];
                
                String message = new String(buffer, 2, packet.getLength() - 2, StandardCharsets.UTF_8);
                processMessage(txID, message, packet.getAddress().getHostAddress(), packet.getPort());
                
            } catch (SocketTimeoutException e) {
                // Check for retransmissions needed
                checkRetransmissions();
                
                if (delay == 0) continue;
                if (System.currentTimeMillis() - startTime > delay) break;
            }
        }
        checkRetransmissions();
    }
    
    private void processMessage(byte[] txID, String message, String senderAddr, int senderPort) throws Exception {
        if (message.length() < 1) return;
        
        char msgType = message.charAt(0);
        String content = message.length() > 1 ? message.substring(1) : "";
        
        int txIDInt = ((txID[0] & 0xFF) << 8) | (txID[1] & 0xFF);
        
        switch(msgType) {
            case 'G': handleNameRequest(txID); break;
            case 'N': handleNearestRequest(txID, content); break;
            case 'E': handleKeyExistenceRequest(txID, content); break;
            case 'R': handleReadRequest(txID, content); break;
            case 'W': handleWriteRequest(txID, content); break;
            case 'C': handleCASRequest(txID, content); break;
            case 'V': handleRelayRequest(txID, content); break;
            case 'H': handleNameResponse(txID, content); break;
            case 'O': handleNearestResponse(txID, content); break;
            case 'F': case 'S': case 'X': case 'D': handleResponse(txIDInt, content); break;
        }
    }
    
    private void handleNameRequest(byte[] txID) throws Exception {
        String response = " " + encodeString(nodeName);
        sendMessage(txID, "H" + response);
    }
    
    private void handleNearestRequest(byte[] txID, String content) throws Exception {
        // Parse hashID from message
        StringBuilder response = new StringBuilder(" ");
        
        List<Map.Entry<String, String>> sorted = new ArrayList<>(addressPairs.entrySet());
        sorted.sort((a, b) -> {
            try {
                byte[] hashA = HashID.computeHashID(a.getKey());
                byte[] hashB = HashID.computeHashID(b.getKey());
                int distA = calculateDistance(nodeHashID, hashA);
                int distB = calculateDistance(nodeHashID, hashB);
                return Integer.compare(distA, distB);
            } catch (Exception e) { return 0; }
        });
        
        for (int i = 0; i < Math.min(3, sorted.size()); i++) {
            response.append(encodeString(sorted.get(i).getKey()));
            response.append(encodeString(sorted.get(i).getValue()));
        }
        
        sendMessage(txID, "O" + response.toString());
    }
    
    private void handleKeyExistenceRequest(byte[] txID, String content) throws Exception {
        DecodeResult decoded = decodeString(content, 0);
        String key = decoded.value;
        
        if (dataPairs.containsKey(key)) {
            sendMessage(txID, " Y ");
        } else {
            byte[] keyHash = HashID.computeHashID(key);
            if (isClosestNode(keyHash)) {
                sendMessage(txID, " N ");
            } else {
                sendMessage(txID, " ? ");
            }
        }
    }
    
    private void handleReadRequest(byte[] txID, String content) throws Exception {
        DecodeResult decoded = decodeString(content, 0);
        String key = decoded.value;
        
        if (dataPairs.containsKey(key)) {
            String value = dataPairs.get(key);
            sendMessage(txID, " Y " + encodeString(value));
        } else {
            byte[] keyHash = HashID.computeHashID(key);
            if (isClosestNode(keyHash)) {
                sendMessage(txID, " N ");
            } else {
                sendMessage(txID, " ? ");
            }
        }
    }
    
    private void handleWriteRequest(byte[] txID, String content) throws Exception {
        DecodeResult key = decodeString(content, 0);
        DecodeResult value = decodeString(content, key.endPos);
        
        byte[] keyHash = HashID.computeHashID(key.value);
        
        if (dataPairs.containsKey(key.value)) {
            dataPairs.put(key.value, value.value);
            sendMessage(txID, " R ");
        } else if (isClosestNode(keyHash)) {
            dataPairs.put(key.value, value.value);
            sendMessage(txID, " A ");
        } else {
            sendMessage(txID, " X ");
        }
    }
    
    private void handleCASRequest(byte[] txID, String content) throws Exception {
        DecodeResult key = decodeString(content, 0);
        DecodeResult current = decodeString(content, key.endPos);
        DecodeResult newVal = decodeString(content, current.endPos);
        
        byte[] keyHash = HashID.computeHashID(key.value);
        
        if (dataPairs.containsKey(key.value)) {
            if (dataPairs.get(key.value).equals(current.value)) {
                dataPairs.put(key.value, newVal.value);
                sendMessage(txID, " R ");
            } else {
                sendMessage(txID, " N ");
            }
        } else if (isClosestNode(keyHash)) {
            dataPairs.put(key.value, newVal.value);
            sendMessage(txID, " A ");
        } else {
            sendMessage(txID, " X ");
        }
    }
    
    private void handleRelayRequest(byte[] txID, String content) throws Exception {
        DecodeResult nodeName = decodeString(content, 0);
        String embeddedMsg = content.substring(nodeName.endPos);
        
        // Send embedded message to target node
        // Response will be returned to sender of relay
        forwardToNode(txID, nodeName.value, embeddedMsg);
    }
    
    private void handleNameResponse(byte[] txID, String content) throws Exception {
        DecodeResult name = decodeString(content, 0);
        String key = name.value;
        // Store address pair if not already present
        if (!addressPairs.containsKey(key)) {
            addressPairs.put(key, "unknown");
        }
    }
    
    private void handleNearestResponse(byte[] txID, String content) throws Exception {
        // Parse address pairs from response and store them
        int pos = 0;
        while (pos < content.length()) {
            DecodeResult key = decodeString(content, pos);
            pos = key.endPos;
            if (pos >= content.length()) break;
            DecodeResult value = decodeString(content, pos);
            pos = value.endPos;
            addressPairs.put(key.value, value.value);
        }
    }
    
    private void handleResponse(int txID, String content) throws Exception {
        // Generic response handler
    }
    
    // ===== Utility Methods =====
    
    private boolean isClosestNode(byte[] keyHash) {
        List<Integer> distances = new ArrayList<>();
        for (String addrKey : addressPairs.keySet()) {
            try {
                byte[] hash = HashID.computeHashID(addrKey);
                distances.add(calculateDistance(hash, keyHash));
            } catch (Exception e) {}
        }
        Collections.sort(distances);
        if (distances.size() < 3) return true;
        int ownDist = calculateDistance(nodeHashID, keyHash);
        return ownDist <= distances.get(2);
    }
    
    private void checkRetransmissions() throws Exception {
        long now = System.currentTimeMillis();
        List<Integer> toRemove = new ArrayList<>();
        
        for (Map.Entry<Integer, PendingRequest> entry : pendingRequests.entrySet()) {
            PendingRequest req = entry.getValue();
            if (now - req.createdTime > 5000) {
                if (req.retryCount < 3) {
                    req.retryCount++;
                    req.createdTime = now;
                    // Resend
                } else {
                    toRemove.add(entry.getKey());
                }
            }
        }
        for (int id : toRemove) {
            pendingRequests.remove(id);
        }
    }
    
    private void sendMessage(byte[] txID, String content) throws Exception {
        byte[] fullMessage = new byte[txID.length + content.length()];
        System.arraycopy(txID, 0, fullMessage, 0, txID.length);
        System.arraycopy(content.getBytes(StandardCharsets.UTF_8), 0, fullMessage, txID.length, content.length());
        // Send to appropriate node or relay
    }
    
    private void forwardToNode(byte[] txID, String nodeName, String message) throws Exception {
        // Lookup node address and send message
    }
    
    private int generateTransactionID() {
        int txid;
        do {
            txid = new Random().nextInt(65536);
        } while ((txid & 0xFF) == 0x20 || ((txid >> 8) & 0xFF) == 0x20);
        return txid;
    }
    
    // ===== Interface Methods =====
    
    public boolean isActive(String nodeName) throws Exception {
        handleIncomingMessages(100);
        return addressPairs.containsKey(nodeName);
    }
    
    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
    }
    
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            relayStack.pop();
        }
    }
    
    public boolean exists(String key) throws Exception {
        handleIncomingMessages(100);
        return dataPairs.containsKey(key);
    }
    
    public String read(String key) throws Exception {
        handleIncomingMessages(100);
        return dataPairs.get(key);
    }
    
    public boolean write(String key, String value) throws Exception {
        dataPairs.put(key, value);
        handleIncomingMessages(100);
        return true;
    }
    
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        if (dataPairs.containsKey(key) && dataPairs.get(key).equals(currentValue)) {
            dataPairs.put(key, newValue);
            handleIncomingMessages(100);
            return true;
        }
        handleIncomingMessages(100);
        return false;
    }
}

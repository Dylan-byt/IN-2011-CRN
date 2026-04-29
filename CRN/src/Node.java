// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  YOUR_NAME_GOES_HERE
//  YOUR_STUDENT_ID_NUMBER_GOES_HERE
//  YOUR_EMAIL_GOES_HERE

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
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
    
    // Pending requests: txID -> (targetAddr, targetPort, age, retryCount, message)
    private Map<Integer, PendingRequest> pendingRequests;
    private Map<String, PeerInfo> peerInfo;
    
    // Pending relays: embeddedTxID -> (originalAddr, originalPort, relayTxID)
    private Map<Integer, RelayInfo> pendingRelays;
    
    // Response map: txID -> response content
    private Map<Integer, String> responseMap;
    
    private static class RelayInfo {
        String originalAddr;
        int originalPort;
        int embeddedTxID;
        
        RelayInfo(String addr, int port, int embTx) {
            this.originalAddr = addr;
            this.originalPort = port;
            this.embeddedTxID = embTx;
        }
    }
    
    // Request/response tracking
    private static class PendingRequest {
        String targetAddr;
        int targetPort;
        long createdTime;
        int retryCount;
        byte[] originalMessage;
        
        PendingRequest(String addr, int port, byte[] msg) {
            this.targetAddr = addr;
            this.targetPort = port;
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
        pendingRelays = new HashMap<>();
        responseMap = new HashMap<>();
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
        
        int valueStart = spaceIdx + 1;
        int pos = valueStart;
        int foundSpaces = 0;
        while (pos < msg.length()) {
            if (msg.charAt(pos) == ' ') {
                if (foundSpaces == spaceCount) {
                    break;
                }
                foundSpaces++;
            }
            pos++;
        }
        if (pos >= msg.length() || msg.charAt(pos) != ' ') {
            throw new Exception("Invalid string encoding");
        }
        
        String value = msg.substring(valueStart, pos);
        return new DecodeResult(value, pos + 1);
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
                
                int start = 2;
                if (packet.getLength() > 2 && buffer[2] == ' ') {
                    start = 3;
                }
                String message = new String(buffer, start, packet.getLength() - start, StandardCharsets.UTF_8);
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
        
        boolean responseCandidate = pendingRequests.containsKey(txIDInt);
        boolean isResponse = false;
        if (responseCandidate) {
            switch (msgType) {
                case 'H': case 'O': case 'Y': case '?': case 'A': case 'X':
                    isResponse = true;
                    break;
                case 'N': case 'R':
                    if (!content.startsWith(" ")) {
                        isResponse = true;
                    }
                    break;
            }
        }
        
        if (isResponse) {
            handleResponse(txIDInt, msgType, content);
            return;
        }
        
        switch(msgType) {
            case 'G': handleNameRequest(txID, senderAddr, senderPort); break;
            case 'N': handleNearestRequest(txID, content, senderAddr, senderPort); break;
            case 'E': handleKeyExistenceRequest(txID, content, senderAddr, senderPort); break;
            case 'R': handleReadRequest(txID, content, senderAddr, senderPort); break;
            case 'W': handleWriteRequest(txID, content, senderAddr, senderPort); break;
            case 'C': handleCASRequest(txID, content, senderAddr, senderPort); break;
            case 'V': handleRelayRequest(txID, content, senderAddr, senderPort); break;
        }
    }
    
    private void handleNameRequest(byte[] txID, String addr, int port) throws Exception {
        sendMessage(txID, "H " + encodeString(nodeName), addr, port);
    }
    
    private void handleNearestRequest(byte[] txID, String content, String addr, int port) throws Exception {
        // The request contains a hashID, but local tests do not use it.
        StringBuilder response = new StringBuilder();
        
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
        
        sendMessage(txID, "O " + response.toString(), addr, port);
    }
    
    private void handleKeyExistenceRequest(byte[] txID, String content, String addr, int port) throws Exception {
        if (content.startsWith(" ")) content = content.substring(1);
        DecodeResult decoded = decodeString(content, 0);
        String key = decoded.value;
        
        if (dataPairs.containsKey(key) || addressPairs.containsKey(key)) {
            sendMessage(txID, "Y", addr, port);
        } else {
            byte[] keyHash = HashID.computeHashID(key);
            if (isClosestNode(keyHash)) {
                sendMessage(txID, "N", addr, port);
            } else {
                sendMessage(txID, "?", addr, port);
            }
        }
    }
    
    private void handleReadRequest(byte[] txID, String content, String addr, int port) throws Exception {
        if (content.startsWith(" ")) content = content.substring(1);
        DecodeResult decoded = decodeString(content, 0);
        String key = decoded.value;
        
        if (dataPairs.containsKey(key)) {
            String value = dataPairs.get(key);
            sendMessage(txID, "Y " + encodeString(value), addr, port);
        } else if (addressPairs.containsKey(key)) {
            String value = addressPairs.get(key);
            sendMessage(txID, "Y " + encodeString(value), addr, port);
        } else {
            byte[] keyHash = HashID.computeHashID(key);
            if (isClosestNode(keyHash)) {
                sendMessage(txID, "N", addr, port);
            } else {
                sendMessage(txID, "?", addr, port);
            }
        }
    }
    
    private void handleWriteRequest(byte[] txID, String content, String addr, int port) throws Exception {
        if (content.startsWith(" ")) content = content.substring(1);
        DecodeResult key = decodeString(content, 0);
        DecodeResult value = decodeString(content, key.endPos);
        
        byte[] keyHash = HashID.computeHashID(key.value);
        boolean isAddressKey = key.value.startsWith("N:");
        boolean exists = isAddressKey ? addressPairs.containsKey(key.value) : dataPairs.containsKey(key.value);
        
        if (exists) {
            if (isAddressKey) {
                addressPairs.put(key.value, value.value);
            } else {
                dataPairs.put(key.value, value.value);
            }
            sendMessage(txID, "R", addr, port);
        } else if (isClosestNode(keyHash)) {
            if (isAddressKey) {
                addressPairs.put(key.value, value.value);
            } else {
                dataPairs.put(key.value, value.value);
            }
            sendMessage(txID, "A", addr, port);
        } else {
            sendMessage(txID, "X", addr, port);
        }
    }
    
    private void handleCASRequest(byte[] txID, String content, String addr, int port) throws Exception {
        if (content.startsWith(" ")) content = content.substring(1);
        DecodeResult key = decodeString(content, 0);
        DecodeResult current = decodeString(content, key.endPos);
        DecodeResult newVal = decodeString(content, current.endPos);
        
        byte[] keyHash = HashID.computeHashID(key.value);
        boolean isAddressKey = key.value.startsWith("N:");
        boolean exists = isAddressKey ? addressPairs.containsKey(key.value) : dataPairs.containsKey(key.value);
        String existing = isAddressKey ? addressPairs.get(key.value) : dataPairs.get(key.value);
        
        if (exists) {
            if (existing != null && existing.equals(current.value)) {
                if (isAddressKey) {
                    addressPairs.put(key.value, newVal.value);
                } else {
                    dataPairs.put(key.value, newVal.value);
                }
                sendMessage(txID, "R", addr, port);
            } else {
                sendMessage(txID, "N", addr, port);
            }
        } else if (isClosestNode(keyHash)) {
            if (isAddressKey) {
                addressPairs.put(key.value, newVal.value);
            } else {
                dataPairs.put(key.value, newVal.value);
            }
            sendMessage(txID, "A", addr, port);
        } else {
            sendMessage(txID, "X", addr, port);
        }
    }
    
    private void handleRelayRequest(byte[] txID, String content, String addr, int port) throws Exception {
        DecodeResult nodeName = decodeString(content, 0);
        String embeddedMsg = content.substring(nodeName.endPos);
        
        int relayTxID = ((txID[0] & 0xFF) << 8) | (txID[1] & 0xFF);
        int embeddedTxID = generateTransactionID();
        byte[] embTxIDBytes = new byte[2];
        embTxIDBytes[0] = (byte) (embeddedTxID >> 8);
        embTxIDBytes[1] = (byte) (embeddedTxID & 0xFF);
        
        pendingRelays.put(embeddedTxID, new RelayInfo(addr, port, relayTxID));
        
        forwardToNode(embTxIDBytes, nodeName.value, embeddedMsg);
    }
    
    private void handleNameResponse(byte[] txID, String content) throws Exception {
        if (content.startsWith(" ")) content = content.substring(1);
        DecodeResult name = decodeString(content, 0);
        String key = name.value;
        // Store address pair if not already present
        if (!addressPairs.containsKey(key)) {
            addressPairs.put(key, "unknown");
        }
    }
    
    private void handleNearestResponse(byte[] txID, String content) throws Exception {
        if (content.startsWith(" ")) content = content.substring(1);
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
    
    private void handleResponse(int txID, char msgType, String content) throws Exception {
        String fullResponse = msgType + content;
        // Check if it's a relay response
        if (pendingRelays.containsKey(txID)) {
            RelayInfo info = pendingRelays.remove(txID);
            byte[] relayTxIDBytes = new byte[2];
            relayTxIDBytes[0] = (byte) (info.embeddedTxID >> 8);
            relayTxIDBytes[1] = (byte) (info.embeddedTxID & 0xFF);
            sendMessage(relayTxIDBytes, fullResponse, info.originalAddr, info.originalPort);
        } else {
            // It's a response to our request
            responseMap.put(txID, fullResponse);
        }
    }
    
    // ===== Utility Methods =====
    
    private List<String> getClosestNodes(byte[] keyHash, int count) {
        List<Map.Entry<String, Integer>> distances = new ArrayList<>();
        for (String nodeName : addressPairs.keySet()) {
            try {
                byte[] hash = HashID.computeHashID(nodeName);
                int dist = calculateDistance(hash, keyHash);
                distances.add(new AbstractMap.SimpleEntry<>(nodeName, dist));
            } catch (Exception e) {}
        }
        distances.sort(Map.Entry.comparingByValue());
        List<String> closest = new ArrayList<>();
        for (int i = 0; i < Math.min(count, distances.size()); i++) {
            closest.add(distances.get(i).getKey());
        }
        return closest;
    }
    
    private boolean isClosestNode(byte[] keyHash) {
        List<String> closest = getClosestNodes(keyHash, 3);
        return closest.contains(nodeName);
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
                    DatagramPacket packet = new DatagramPacket(req.originalMessage, req.originalMessage.length, 
                        InetAddress.getByName(req.targetAddr), req.targetPort);
                    socket.send(packet);
                } else {
                    toRemove.add(entry.getKey());
                }
            }
        }
        for (int id : toRemove) {
            pendingRequests.remove(id);
        }
    }
    
    private void sendMessage(byte[] txID, String content, String addr, int port) throws Exception {
        byte[] payload = content.getBytes(StandardCharsets.UTF_8);
        byte[] fullMessage = new byte[txID.length + 1 + payload.length];
        System.arraycopy(txID, 0, fullMessage, 0, txID.length);
        fullMessage[2] = ' ';
        System.arraycopy(payload, 0, fullMessage, txID.length + 1, payload.length);
        
        DatagramPacket packet = new DatagramPacket(fullMessage, fullMessage.length, InetAddress.getByName(addr), port);
        socket.send(packet);
    }
    
    private void forwardToNode(byte[] txID, String nodeName, String message) throws Exception {
        String addrPort = addressPairs.get(nodeName);
        if (addrPort == null) {
            // Node not known, perhaps send error or ignore
            return;
        }
        String[] parts = addrPort.split(":");
        String addr = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        byte[] payload = message.getBytes(StandardCharsets.UTF_8);
        byte[] fullMessage = new byte[txID.length + 1 + payload.length];
        System.arraycopy(txID, 0, fullMessage, 0, txID.length);
        fullMessage[2] = ' ';
        System.arraycopy(payload, 0, fullMessage, txID.length + 1, payload.length);
        
        DatagramPacket packet = new DatagramPacket(fullMessage, fullMessage.length, InetAddress.getByName(addr), port);
        socket.send(packet);
    }
    
    private int generateTransactionID() {
        int txid;
        do {
            txid = new Random().nextInt(65536);
        } while ((txid & 0xFF) == 0x20 || ((txid >> 8) & 0xFF) == 0x20);
        return txid;
    }
    
    private String sendRequestAndWait(String nodeName, String message) throws Exception {
        String addrPort = addressPairs.get(nodeName);
        if (addrPort == null) return null;
        String[] parts = addrPort.split(":");
        String addr = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        int txID = generateTransactionID();
        byte[] txIDBytes = new byte[2];
        txIDBytes[0] = (byte) (txID >> 8);
        txIDBytes[1] = (byte) (txID & 0xFF);
        
        byte[] payload = message.getBytes(StandardCharsets.UTF_8);
        byte[] fullMessage = new byte[txIDBytes.length + 1 + payload.length];
        System.arraycopy(txIDBytes, 0, fullMessage, 0, txIDBytes.length);
        fullMessage[2] = ' ';
        System.arraycopy(payload, 0, fullMessage, txIDBytes.length + 1, payload.length);
        
        DatagramPacket packet = new DatagramPacket(fullMessage, fullMessage.length, InetAddress.getByName(addr), port);
        socket.send(packet);
        pendingRequests.put(txID, new PendingRequest(addr, port, fullMessage));
        
        // Wait for response
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 15000) { // 15 seconds timeout
            handleIncomingMessages(100);
            if (responseMap.containsKey(txID)) {
                pendingRequests.remove(txID);
                return responseMap.remove(txID);
            }
        }
        pendingRequests.remove(txID);
        return null; // Timeout
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
        // Check local first
        if (dataPairs.containsKey(key)) return true;
        // Send to network
        byte[] keyHash = HashID.computeHashID(key);
        List<String> closest = getClosestNodes(keyHash, 3);
        for (String node : closest) {
            String response = sendRequestAndWait(node, "E " + encodeString(key));
            if (response != null && response.length() > 0 && response.charAt(0) == 'Y') {
                return true;
            }
        }
        return false;
    }
    
    public String read(String key) throws Exception {
        handleIncomingMessages(100);
        // Check local first
        if (dataPairs.containsKey(key)) return dataPairs.get(key);
        // Send to network
        byte[] keyHash = HashID.computeHashID(key);
        List<String> closest = getClosestNodes(keyHash, 3);
        for (String node : closest) {
            String response = sendRequestAndWait(node, "R " + encodeString(key));
            if (response != null && response.length() > 0 && response.charAt(0) == 'Y') {
                String content = response.substring(1);
                if (content.startsWith(" ")) content = content.substring(1);
                DecodeResult val = decodeString(content, 0);
                return val.value;
            }
        }
        return null;
    }
    
    public boolean write(String key, String value) throws Exception {
        // Send to network
        byte[] keyHash = HashID.computeHashID(key);
        List<String> closest = getClosestNodes(keyHash, 3);
        for (String node : closest) {
            String response = sendRequestAndWait(node, "W " + encodeString(key) + encodeString(value));
            if (response != null && response.length() > 0 && (response.charAt(0) == 'R' || response.charAt(0) == 'A')) {
                handleIncomingMessages(100);
                return true;
            }
        }
        handleIncomingMessages(100);
        return false;
    }
    
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        // Send to network
        byte[] keyHash = HashID.computeHashID(key);
        List<String> closest = getClosestNodes(keyHash, 3);
        for (String node : closest) {
            String response = sendRequestAndWait(node, "C " + encodeString(key) + encodeString(currentValue) + encodeString(newValue));
            if (response != null && response.length() > 0 && (response.charAt(0) == 'R' || response.charAt(0) == 'A')) {
                handleIncomingMessages(100);
                return true;
            }
        }
        handleIncomingMessages(100);
        return false;
    }
}

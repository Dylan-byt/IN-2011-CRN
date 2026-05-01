import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  Dylan Johnson
//  240032948
//  dylan.johnson@city.ac.uk

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
    private static final boolean DEBUG = false;
    private static final int RETRANSMIT_INTERVAL_MS = 5000;
    private static final int MAX_RETRIES = 3;
    private static final int SOCKET_TIMEOUT_MS = 100;
    private static final long STALE_ADDRESS_TIMEOUT_MS = 30000;
    private static final int MAX_ADDRESSES_PER_DISTANCE = 3;
    private static final int BOOTSTRAP_PROBE_TIMEOUT_MS = 12000;
    private static final int BOOTSTRAP_PORT_START = 20110;
    private static final int BOOTSTRAP_PORT_END = 20130;

    private String nodeName;
    private byte[] nodeHashID;
    private DatagramSocket socket;
    private int port;

    private final Stack<String> relayStack;
    private final Map<String, String> addressPairs;
    private final Map<String, Long> addressLastSeen;
    private final Map<String, String> dataPairs;

    private final Map<Integer, PendingRequest> pendingRequests;
    private final Map<Integer, RelayInfo> pendingRelays;
    private final Map<Integer, String> responseMap;

    private final Random random = new Random();

    // Stores information needed to return a relayed response to the original sender
    private static class RelayInfo {
        String originalAddress;
        int originalPort;
        int relayTxID;

        RelayInfo(String addr, int port, int relayTxID) {
            this.originalAddress = addr;
            this.originalAddress = addr;
            this.originalPort = port;
            this.relayTxID = relayTxID;
        }
    }

    // Used to debug various partsd of the system. I used it for the smoke test as i wasn't sure why it wasn't working.
    // if you want to see it go then, just set DEBUG to true at the top of the class. It will print out all messages sent and received, as well as learned addresses.
    private void debug(String msg) {
        if (DEBUG) {
            System.out.println("[NODE DEBUG] " + msg);
        }
    }

    // Stores information about a request waiting for a response
    private static class PendingRequest {
        String targetAddress;
        int targetPort;
        byte[] originalMessage;
        int retryCount;
        long nextRetryTime;
        String logicalNodeName;
        boolean isRelayed;

        PendingRequest(String address, int port, byte[] msg, String logicalNodeName, boolean isRelayed) {
            this.targetAddress = address;
            this.targetPort = port;
            this.originalMessage = msg;
            this.retryCount = 0;
            this.nextRetryTime = System.currentTimeMillis() + RETRANSMIT_INTERVAL_MS;
            this.logicalNodeName = logicalNodeName;
            this.isRelayed = isRelayed;
        }
    }

    //Stores the result of decoding a CRN string
    private static class DecodeResult {
        String value;
        int endPos;

        DecodeResult(String value, int endPos) {
            this.value = value;
            this.endPos = endPos;
        }
    }

    public Node() {
        relayStack = new Stack<>();
        addressPairs = new ConcurrentHashMap<>();
        addressLastSeen = new ConcurrentHashMap<>();
        dataPairs = new ConcurrentHashMap<>();
        pendingRequests = new ConcurrentHashMap<>();
        pendingRelays = new ConcurrentHashMap<>();
        responseMap = new ConcurrentHashMap<>();
    }

    // ===== Configuration Methods =====

    //Sets this node's CRN node name and calculates its hash ID.
    public void setNodeName(String name) throws Exception {
        this.nodeName = name;
        this.nodeHashID = HashID.computeHashID(name);
    }

    // Opens a UDP socket on the given port and records the local address for this node.
    public void openPort(int portNumber) throws Exception {
        this.port = portNumber;
        this.socket = new DatagramSocket(portNumber);
        this.socket.setSoTimeout(SOCKET_TIMEOUT_MS);

        String localAddress = getBestLocalAddress();
        recordAddress(nodeName, localAddress + ":" + port);
    }


    // Records own node address. Was one of the methods i had to get the smoke test to work, as boot strap relied on nodes knowing their own address to work
    public void addOwnAddress(String ipAddress, int portNumber) throws Exception {
        recordAddress(nodeName, ipAddress + ":" + portNumber);
    }

    // Used for testing to see what addresses the node currently knows about.
    public void debugKnownAddresses() {
        System.out.println("Known address pairs:");
        for (Map.Entry<String, String> entry : addressPairs.entrySet()) {
            System.out.println("  " + entry.getKey() + " -> " + entry.getValue());
        }
    }

    // This method tries to find the best local IP address to use for bootstrapping and communication. Mainly for smoke testing
    private String getBestLocalAddress() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();

            while (interfaces.hasMoreElements()) {
                NetworkInterface ni = interfaces.nextElement();

                if (!ni.isUp() || ni.isLoopback()) {
                    continue;
                }

                Enumeration<InetAddress> addresses = ni.getInetAddresses();

                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    String ip = addr.getHostAddress();

                    if (ip.startsWith("10.") && !ip.contains(":")) {
                        return ip;
                    }
                }
            }
        } catch (Exception e) {
        }

        try {
            String ip = InetAddress.getLocalHost().getHostAddress();
            if (ip != null && !ip.contains(":")) {
                return ip;
            }
        } catch (Exception e) {
        }

        return "127.0.0.1";
    }

    // ===== CRN String Encoding / Decoding =====

    // Encodes a Java string into the CRN string format:
    // number of spaces + " " + string value + " "
    private String encodeString(String s) {
        int spaceCount = 0;

        for (char c : s.toCharArray()) {
            if (c == ' ') {
                spaceCount++;
            }
        }

        return spaceCount + " " + s + " ";
    }

    // Decodes a CRN string starting from the given position in the message.
    // Returns the decoded string and the position of the next character after the string.
    private DecodeResult decodeString(String msg, int startPos) throws Exception {
        int spaceIdx = msg.indexOf(' ', startPos);

        if (spaceIdx == -1) {
            throw new Exception("Invalid string encoding");
        }

        String countStr = msg.substring(startPos, spaceIdx);
        int requiredSpaces = Integer.parseInt(countStr);

        int valueStart = spaceIdx + 1;
        int pos = valueStart;
        int foundSpaces = 0;

        while (pos < msg.length()) {
            if (msg.charAt(pos) == ' ') {
                if (foundSpaces == requiredSpaces) {
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

    // Removes a leading space from the content if it exists.
    private String stripLeadingSpace(String content) {
        if (content.startsWith(" ")) {
            return content.substring(1);
        }
        return content;
    }

    // Converts a 64-character hexadecimal string into a 32-byte array.
    private byte[] hexStringToBytes(String hex) throws Exception {
        if (hex.length() != 64) {
            throw new Exception("Invalid hashID length");
        }

        byte[] bytes = new byte[32];

        for (int i = 0; i < 32; i++) {
            int hi = Character.digit(hex.charAt(i * 2), 16);
            int lo = Character.digit(hex.charAt(i * 2 + 1), 16);

            if (hi < 0 || lo < 0) {
                throw new Exception("Invalid hashID character");
            }

            bytes[i] = (byte) ((hi << 4) | lo);
        }

        return bytes;
    }

    // Converts a byte array into a hexadecimal string.
    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();

        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }

        return sb.toString();
    }

    // ===== Distance Calculation =====

    // Calculates the distance between two hash IDs as defined in the RFC.
    // distance is 256 minus the number of matching leading bits.
    private int calculateDistance(byte[] hash1, byte[] hash2) {
        int matchingBits = 0;

        for (int i = 0; i < hash1.length && i < hash2.length; i++) {
            int xor = (hash1[i] ^ hash2[i]) & 0xFF;

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

    // ===== UDP Packet Helpers =====

    // Builds a complete UDP packet using the CRN format:
    // two-byte transaction ID, one space, then the message body.
    private byte[] buildPacket(byte[] txID, String content) {
        byte[] payload = content.getBytes(StandardCharsets.UTF_8);

        byte[] fullMessage = new byte[txID.length + 1 + payload.length];

        System.arraycopy(txID, 0, fullMessage, 0, txID.length);
        fullMessage[2] = ' ';
        System.arraycopy(payload, 0, fullMessage, 3, payload.length);

        return fullMessage;
    }

    // Sends a message to the given address and port with the specified transaction ID.
    private void sendMessage(byte[] txID, String content, String addr, int port) throws Exception {
        byte[] fullMessage = buildPacket(txID, content);

        int txIDInt = ((txID[0] & 0xFF) << 8) | (txID[1] & 0xFF);

        debug("SEND to " + addr + ":" + port
                + " tx=" + txIDInt
                + " msg=[" + content + "]");

        DatagramPacket packet = new DatagramPacket(
                fullMessage,
                fullMessage.length,
                InetAddress.getByName(addr),
                port
        );

        socket.send(packet);
    }

    private int generateTransactionID() {
        int txid;

        do {
            txid = random.nextInt(65536);
        } while ((txid & 0xFF) == 0x20 || ((txid >> 8) & 0xFF) == 0x20);

        return txid;
    }

    // ===== Message Handling =====

    // Handles incoming UDP packets for up to delay milliseconds. If delay is zero, waits indefinitely until a packet is received.
    public void handleIncomingMessages(int delay) throws Exception {
        long startTime = System.currentTimeMillis();

        while (true) {
            long elapsed = System.currentTimeMillis() - startTime;

            if (delay > 0 && elapsed >= delay) {
                break;
            }

            try {
                byte[] buffer = new byte[65536];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                if (packet.getLength() < 3) {
                    continue;
                }

                String senderAddr = packet.getAddress().getHostAddress();
                int senderPort = packet.getPort();

                byte[] txID = new byte[2];
                txID[0] = buffer[0];
                txID[1] = buffer[1];

                int start = 2;

                if (packet.getLength() > 2 && buffer[2] == ' ') {
                    start = 3;
                }

                String message = new String(
                buffer,
                start,
                packet.getLength() - start,
                StandardCharsets.UTF_8
        );

        int txIDInt = ((txID[0] & 0xFF) << 8) | (txID[1] & 0xFF);

        debug("RECV from " + senderAddr + ":" + senderPort
                + " tx=" + txIDInt
                + " msg=[" + message + "]");

        if (!message.isEmpty()) {
            char msgType = message.charAt(0);

            if (msgType == 'G'
                    || msgType == 'N'
                    || msgType == 'E'
                    || msgType == 'R'
                    || msgType == 'W'
                    || msgType == 'C'
                    || msgType == 'V') {
                requestNameFromPeer(senderAddr, senderPort);
            }
        }

        try {
            processMessage(txID, message, senderAddr, senderPort);
        } catch (Exception e) {
            debug("IGNORED malformed/problem message from "
                    + senderAddr + ":" + senderPort
                    + " msg=[" + message + "] error=" + e.getMessage());
        }

            } catch (SocketTimeoutException e) {
                checkRetransmissions();
                cleanupStaleAddresses();

                if (delay == 0) {
                    continue;
                }

                if (System.currentTimeMillis() - startTime >= delay) {
                    break;
                }
            }
        }

        checkRetransmissions();
        cleanupStaleAddresses();
    }

    // Processes a received message based on its type and content.
    private void processMessage(byte[] txID, String message, String senderAddr, int senderPort) throws Exception {
        if (message == null || message.length() < 1) {
            return;
        }

        char msgType = message.charAt(0);
        String content = message.length() > 1 ? message.substring(1) : "";

        int txIDInt = ((txID[0] & 0xFF) << 8) | (txID[1] & 0xFF);

        if (pendingRequests.containsKey(txIDInt) || pendingRelays.containsKey(txIDInt)) {
            if (isResponseType(msgType)) {
                handleResponse(txIDInt, msgType, content, senderAddr, senderPort);
                return;
            }
        }

        switch (msgType) {
            case 'G':
                handleNameRequest(txID, senderAddr, senderPort);
                break;

            case 'H':
                handleNameResponse(content, senderAddr, senderPort);
                break;

            case 'N':
                handleNearestRequest(txID, content, senderAddr, senderPort);
                break;

            case 'O':
                handleNearestResponse(content);
                break;

            case 'E':
                handleKeyExistenceRequest(txID, content, senderAddr, senderPort);
                break;

            case 'R':
                handleReadRequest(txID, content, senderAddr, senderPort);
                break;

            case 'W':
                handleWriteRequest(txID, content, senderAddr, senderPort);
                break;

            case 'C':
                handleCASRequest(txID, content, senderAddr, senderPort);
                break;

            case 'V':
                handleRelayRequest(txID, content, senderAddr, senderPort);
                break;

            case 'I':
                
                break;

            default:
               
                break;
        }
    }

    // Returns true if the message type is a CRN response type.
    private boolean isResponseType(char msgType) {
    return msgType == 'H'
            || msgType == 'O'
            || msgType == 'F'
            || msgType == 'S'
            || msgType == 'X'
            || msgType == 'D'
            || msgType == 'Y'
            || msgType == 'N'
            || msgType == '?'
            || msgType == 'A'
            || msgType == 'R';
    }

    // Handles a name request and replies with this node's name.
    private void handleNameRequest(byte[] txID, String addr, int port) throws Exception {
        sendMessage(txID, "H " + encodeString(nodeName), addr, port);
    }

    // Handles a name response and records the sender's node name and address.
    private void handleNameResponse(String content, String senderAddr, int senderPort) throws Exception {
        content = stripLeadingSpace(content);

        try {
            DecodeResult decoded = decodeString(content, 0);

            if (decoded.value.startsWith("N:")) {
                recordAddress(decoded.value, senderAddr + ":" + senderPort);
            }
        } catch (Exception e) {
            
        }
    }

    // Handles a nearest-node request and replies with up to three known address pairs closest to the target hash.
    private void handleNearestRequest(byte[] txID, String content, String addr, int port) throws Exception {
        String hashText = stripLeadingSpace(content);

        byte[] targetHash;

        try {
            targetHash = hexStringToBytes(hashText);
        } catch (Exception e) {
            return;
        }

        List<Map.Entry<String, String>> sorted = new ArrayList<>();

        for (Map.Entry<String, String> entry : addressPairs.entrySet()) {
           
            if (entry.getKey().startsWith("N:")) {
                sorted.add(entry);
            }
        }

        sorted.sort((a, b) -> {
            try {
                byte[] hashA = HashID.computeHashID(a.getKey());
                byte[] hashB = HashID.computeHashID(b.getKey());

                int distA = calculateDistance(hashA, targetHash);
                int distB = calculateDistance(hashB, targetHash);

                return Integer.compare(distA, distB);
            } catch (Exception e) {
                return 0;
            }
        });

        StringBuilder response = new StringBuilder();

        for (int i = 0; i < Math.min(3, sorted.size()); i++) {
            response.append(encodeString(sorted.get(i).getKey()));
            response.append(encodeString(sorted.get(i).getValue()));
        }

        sendMessage(txID, "O " + response, addr, port);
    }

    //Handles a nearest-node response and records any address pairs it contains.
    private void handleNearestResponse(String content) throws Exception {
        content = stripLeadingSpace(content);

        int pos = 0;

        while (pos < content.length()) {
            DecodeResult key = decodeString(content, pos);
            pos = key.endPos;

            if (pos >= content.length()) {
                break;
            }

            DecodeResult value = decodeString(content, pos);
            pos = value.endPos;

            if (key.value.startsWith("N:")) {
                recordAddress(key.value, value.value);
            }
        }
    }

    // Handles an existence request for a key and replies using F Y, F N, or F ?.
    private void handleKeyExistenceRequest(byte[] txID, String content, String addr, int port) throws Exception {
        content = stripLeadingSpace(content);

        DecodeResult decoded = decodeString(content, 0);
        String key = decoded.value;

        boolean exists = dataPairs.containsKey(key) || addressPairs.containsKey(key);

        if (exists) {
            sendMessage(txID, "F Y", addr, port);
            return;
        }

        byte[] keyHash = HashID.computeHashID(key);

        if (isClosestNode(keyHash)) {
            sendMessage(txID, "F N", addr, port);
        } else {
            sendMessage(txID, "F ?", addr, port);
        }
    }

    // Handles a read request for a key and replies with S Y value, S N, or S ?.
    private void handleReadRequest(byte[] txID, String content, String addr, int port) throws Exception {
        content = stripLeadingSpace(content);

        DecodeResult decoded = decodeString(content, 0);
        String key = decoded.value;

        if (dataPairs.containsKey(key)) {
            sendMessage(txID, "S Y " + encodeString(dataPairs.get(key)), addr, port);
            return;
        }

        if (addressPairs.containsKey(key)) {
            sendMessage(txID, "S Y " + encodeString(addressPairs.get(key)), addr, port);
            return;
        }

        byte[] keyHash = HashID.computeHashID(key);

        if (isClosestNode(keyHash)) {
            sendMessage(txID, "S N", addr, port);
        } else {
            sendMessage(txID, "S ?", addr, port);
        }
    }

    // Handles a write request for a key and replies with X A, X R, or X X.
    private void handleWriteRequest(byte[] txID, String content, String addr, int port) throws Exception {
        content = stripLeadingSpace(content);

        DecodeResult key = decodeString(content, 0);
        DecodeResult value = decodeString(content, key.endPos);

        String keyName = key.value;
        String valueString = value.value;

        boolean isAddressKey = keyName.startsWith("N:");
        boolean isDataKey = keyName.startsWith("D:");

        if (!isAddressKey && !isDataKey) {
            sendMessage(txID, "X X", addr, port);
            return;
        }

        boolean exists = isAddressKey
                ? addressPairs.containsKey(keyName)
                : dataPairs.containsKey(keyName);

        if (exists) {
            if (isAddressKey) {
                recordAddress(keyName, valueString);
            } else {
                dataPairs.put(keyName, valueString);
            }

            sendMessage(txID, "X R", addr, port);
            return;
        }

        byte[] keyHash = HashID.computeHashID(keyName);

        if (isClosestNode(keyHash)) {
            if (isAddressKey) {
                recordAddress(keyName, valueString);
            } else {
                dataPairs.put(keyName, valueString);
            }

            sendMessage(txID, "X A", addr, port);
        } else {
            sendMessage(txID, "X X", addr, port);
        }
    }

    // Handles a compare-and-swap request atomically and replies using D R, D N,
    // D A, or D X.
    private void handleCASRequest(byte[] txID, String content, String addr, int port) throws Exception {
        content = stripLeadingSpace(content);

        DecodeResult key = decodeString(content, 0);
        DecodeResult current = decodeString(content, key.endPos);
        DecodeResult newVal = decodeString(content, current.endPos);

        String keyName = key.value;
        String currentValue = current.value;
        String newValue = newVal.value;

        boolean isAddressKey = keyName.startsWith("N:");
        boolean isDataKey = keyName.startsWith("D:");

        if (!isAddressKey && !isDataKey) {
            sendMessage(txID, "D X", addr, port);
            return;
        }

        synchronized (this) {
            boolean exists = isAddressKey
                    ? addressPairs.containsKey(keyName)
                    : dataPairs.containsKey(keyName);

            String existingValue = isAddressKey
                    ? addressPairs.get(keyName)
                    : dataPairs.get(keyName);

            if (exists) {
                if (existingValue != null && existingValue.equals(currentValue)) {
                    if (isAddressKey) {
                        recordAddress(keyName, newValue);
                    } else {
                        dataPairs.put(keyName, newValue);
                    }

                    sendMessage(txID, "D R", addr, port);
                } else {
                    sendMessage(txID, "D N", addr, port);
                }

                return;
            }

            byte[] keyHash = HashID.computeHashID(keyName);

            if (isClosestNode(keyHash)) {
                if (isAddressKey) {
                    recordAddress(keyName, newValue);
                } else {
                    dataPairs.put(keyName, newValue);
                }

                sendMessage(txID, "D A", addr, port);
            } else {
                sendMessage(txID, "D X", addr, port);
            }
        }
    }

    //Handles a relay request by forwarding the embedded message to the target node.
    private void handleRelayRequest(byte[] txID, String content, String addr, int port) throws Exception {
        content = stripLeadingSpace(content);

        DecodeResult nodeNameResult = decodeString(content, 0);
        String targetNode = nodeNameResult.value;
        String embeddedMessage = content.substring(nodeNameResult.endPos);

        int relayTxID = ((txID[0] & 0xFF) << 8) | (txID[1] & 0xFF);

        int embeddedTxID = generateTransactionID();

        byte[] embeddedTxIDBytes = new byte[2];
        embeddedTxIDBytes[0] = (byte) (embeddedTxID >> 8);
        embeddedTxIDBytes[1] = (byte) (embeddedTxID & 0xFF);

        pendingRelays.put(embeddedTxID, new RelayInfo(addr, port, relayTxID));

        forwardToNode(embeddedTxIDBytes, targetNode, embeddedMessage);
    }

    //Handles a response to one of this node's pending requests.
    private void handleResponse(int txID, char msgType, String content, String senderAddr, int senderPort) throws Exception {
        String fullResponse = msgType + content;

        if (pendingRelays.containsKey(txID)) {
            RelayInfo info = pendingRelays.remove(txID);

            byte[] relayTxIDBytes = new byte[2];
            relayTxIDBytes[0] = (byte) (info.relayTxID >> 8);
            relayTxIDBytes[1] = (byte) (info.relayTxID & 0xFF);

            sendMessage(relayTxIDBytes, fullResponse, info.originalAddress, info.originalPort);
            return;
        }

        PendingRequest req = pendingRequests.get(txID);

        if (req != null) {
            if (msgType == 'H' && !req.isRelayed) {
                handleNameResponse(content, senderAddr, senderPort);
            } else if (msgType == 'O') {
                handleNearestResponse(content);
            }
        }

        responseMap.put(txID, fullResponse);
    }

    // ===== Address Table Management =====

    // Records an address key/value pair if it is valid
    // Enforces the limit of three address pairs per distance
    private void recordAddress(String key, String addrPort) throws Exception {
        if (key == null || !key.startsWith("N:")) {
            return;
        }

        if (!isValidAddressValue(addrPort)) {
            return;
        }

        long now = System.currentTimeMillis();

        if (key.equals(nodeName)) {
            addressPairs.put(key, addrPort);
            addressLastSeen.put(key, now);
            debug("LEARNED ADDRESS " + key + " -> " + addrPort);
            return;
        }

        byte[] hash = HashID.computeHashID(key);
        int distance = calculateDistance(nodeHashID, hash);

        if (addressPairs.containsKey(key)) {
            addressPairs.put(key, addrPort);
            addressLastSeen.put(key, now);
            return;
        }

        List<String> sameDistance = new ArrayList<>();

        for (String existing : addressPairs.keySet()) {
            if (existing.equals(nodeName)) {
                continue;
            }

            if (!existing.startsWith("N:")) {
                continue;
            }

            byte[] existingHash = HashID.computeHashID(existing);
            int existingDistance = calculateDistance(nodeHashID, existingHash);

            if (existingDistance == distance) {
                sameDistance.add(existing);
            }
        }

        if (sameDistance.size() < MAX_ADDRESSES_PER_DISTANCE) {
            addressPairs.put(key, addrPort);
            addressLastSeen.put(key, now);
            return;
        }

        String oldestKey = null;
        long oldestSeen = Long.MAX_VALUE;

        for (String existing : sameDistance) {
            long seen = addressLastSeen.getOrDefault(existing, 0L);

            if (seen < oldestSeen) {
                oldestSeen = seen;
                oldestKey = existing;
            }
        }

        if (oldestKey != null) {
            addressPairs.remove(oldestKey);
            addressLastSeen.remove(oldestKey);

            addressPairs.put(key, addrPort);
            addressLastSeen.put(key, now);
        }
    }

    //Checks whether an address value is in the form IP:port.
    private boolean isValidAddressValue(String addrPort) {
        if (addrPort == null) {
            return false;
        }

        String[] parts = addrPort.split(":");

        if (parts.length != 2) {
            return false;
        }

        try {
            int p = Integer.parseInt(parts[1]);
            return p >= 1 && p <= 65535;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // Removes addresses that haven't been seen in a while to prevent the address table from filling up with stale entries.
    private void cleanupStaleAddresses() {
        long now = System.currentTimeMillis();
        List<String> removeKeys = new ArrayList<>();

        for (Map.Entry<String, Long> entry : addressLastSeen.entrySet()) {
            String key = entry.getKey();

            if (key.equals(nodeName)) {
                continue;
            }

            if (now - entry.getValue() > STALE_ADDRESS_TIMEOUT_MS) {
                removeKeys.add(key);
            }
        }

        for (String key : removeKeys) {
            addressLastSeen.remove(key);
            addressPairs.remove(key);
        }
    }

    // Returns true if this node knows at least one other real N: node.
    private boolean hasKnownPeers() {
        for (String key : addressPairs.keySet()) {
            if (!key.equals(nodeName) && key.startsWith("N:")) {
                return true;
            }
        }

        return false;
    }

    //Returns the known nodes closest to the given hash ID.
    private List<String> getClosestNodes(byte[] keyHash, int count) throws Exception {
        List<Map.Entry<String, Integer>> distances = new ArrayList<>();

        for (String name : addressPairs.keySet()) {
            if (!name.startsWith("N:")) {
                continue;
            }

            try {
                byte[] hash = HashID.computeHashID(name);
                int dist = calculateDistance(hash, keyHash);

                distances.add(new AbstractMap.SimpleEntry<>(name, dist));
            } catch (Exception e) {
           
            }
        }

        distances.sort(Map.Entry.comparingByValue());

        List<String> closest = new ArrayList<>();

        for (int i = 0; i < Math.min(count, distances.size()); i++) {
            closest.add(distances.get(i).getKey());
        }

        return closest;
    }

    // Returns true if this node is one of the closest nodes to the given key hash.
    private boolean isClosestNode(byte[] keyHash) throws Exception {
        List<String> closest = getClosestNodes(keyHash, 3);
        return closest.contains(nodeName);
    }

    // ===== Bootstrap / Discovery =====

    //Attempts to discover peers by probing localhost and the local Azure subnet.
    private void bootstrapNetwork() throws Exception {
        if (hasKnownPeers()) {
            return;
        }

        String localIp = getBestLocalAddress();

        List<Integer> probes = new ArrayList<>();

        probeAddressRange("127.0.0.1", probes);

        if (!localIp.equals("127.0.0.1")) {
            probeAddressRange(localIp, probes);
        }

        // Azure subnet probing.
        if (localIp.startsWith("10.")) {
            String[] octets = localIp.split("\\.");

            if (octets.length == 4) {
                String subnet = octets[0] + "." + octets[1] + "." + octets[2] + ".";

                for (int host = 1; host <= 254; host++) {
                    String probeIP = subnet + host;

                    if (probeIP.equals(localIp)) {
                        continue;
                    }

                    probeAddressRange(probeIP, probes);
                }
            }
        }

        long deadline = System.currentTimeMillis() + BOOTSTRAP_PROBE_TIMEOUT_MS;

        while (System.currentTimeMillis() < deadline && !hasKnownPeers()) {
            try {
                handleIncomingMessages(100);
            } catch (Exception e) {
              
            }
        }

        for (Integer txID : probes) {
            pendingRequests.remove(txID);
            responseMap.remove(txID);
        }
    }

    //Sends name requests to all CRN ports on a given IP address.
        private void probeAddressRange(String ipAddress, List<Integer> probes) {
        for (int p = BOOTSTRAP_PORT_START; p <= BOOTSTRAP_PORT_END; p++) {
            try {
                int txID = generateTransactionID();

                debug("PROBE G to " + ipAddress + ":" + p + " tx=" + txID);
                byte[] txIDBytes = new byte[2];
                txIDBytes[0] = (byte) (txID >> 8);
                txIDBytes[1] = (byte) (txID & 0xFF);

                String msg = "G";
                byte[] fullMsg = buildPacket(txIDBytes, msg);

                DatagramPacket pkt = new DatagramPacket(
                        fullMsg,
                        fullMsg.length,
                        InetAddress.getByName(ipAddress),
                        p
                );

                socket.send(pkt);

                pendingRequests.put(
                        txID,
                        new PendingRequest(ipAddress, p, fullMsg, null, false)
                );

                probes.add(txID);
            } catch (Exception e) {
               
            }
        }
    }

    //Attempts to discover the address of a node by asking known close nodes.
    private void discoverNodeAddress(String targetNodeName) throws Exception {
        if (addressPairs.containsKey(targetNodeName)) {
            return;
        }

        bootstrapNetwork();

        byte[] hash = HashID.computeHashID(targetNodeName);
        List<String> closest = getClosestNodes(hash, 3);

        for (String node : closest) {
            if (node.equals(this.nodeName)) {
                continue;
            }

            String response = sendRequestAndWait(node, "N " + bytesToHex(hash));

            if (response != null && response.length() > 0 && response.charAt(0) == 'O') {
                if (addressPairs.containsKey(targetNodeName)) {
                    return;
                }
            }
        }
    }

    // ===== Retransmission =====

    // Checks pending requests and retransmits any request that has timed out
    private void checkRetransmissions() throws Exception {
        long now = System.currentTimeMillis();
        List<Integer> cancelled = new ArrayList<>();

        for (Map.Entry<Integer, PendingRequest> entry : pendingRequests.entrySet()) {
            PendingRequest req = entry.getValue();

            if (now >= req.nextRetryTime) {
                if (req.retryCount < MAX_RETRIES) {
                    req.retryCount++;
                    req.nextRetryTime = now + RETRANSMIT_INTERVAL_MS;

                    DatagramPacket packet = new DatagramPacket(
                            req.originalMessage,
                            req.originalMessage.length,
                            InetAddress.getByName(req.targetAddress),
                            req.targetPort
                    );

                    socket.send(packet);
                } else {
                    cancelled.add(entry.getKey());
                }
            }
        }

        for (int txID : cancelled) {
            pendingRequests.remove(txID);
        }
    }

    // ===== Sending Requests =====

    //Wraps a message in one or more relay messages according to the relay stack
    private String wrapRelayMessage(String original, String finalDestination) throws Exception {
        String wrapped = original;

        for (int i = relayStack.size() - 1; i >= 0; i--) {
            String nextHop;

            if (i == relayStack.size() - 1) {
                nextHop = finalDestination;
            } else {
                nextHop = relayStack.get(i + 1);
            }

            wrapped = "V " + encodeString(nextHop) + wrapped;
        }

        return wrapped;
    }

    //Sends a message directly to a known node name
    private void forwardToNode(byte[] txID, String targetNodeName, String message) throws Exception {
        String addrPort = addressPairs.get(targetNodeName);

        if (addrPort == null) {
            return;
        }

        String[] parts = addrPort.split(":");

        if (parts.length != 2) {
            return;
        }

        String addr = parts[0];
        int targetPort = Integer.parseInt(parts[1]);

        byte[] fullMessage = buildPacket(txID, message);

        DatagramPacket packet = new DatagramPacket(
                fullMessage,
                fullMessage.length,
                InetAddress.getByName(addr),
                targetPort
        );

        socket.send(packet);
    }

    //Sends a request to a node and waits for the matching response, handling incoming messages and retransmissions while waiting. Returns the response body or null if no response is received within the timeout.
    private String sendRequestAndWait(String targetNodeName, String message) throws Exception {
        String firstHop = targetNodeName;

        if (!relayStack.isEmpty()) {
            firstHop = relayStack.firstElement();
        }

        String addrPort = addressPairs.get(firstHop);

        if (addrPort == null && relayStack.isEmpty()) {
            discoverNodeAddress(targetNodeName);
            addrPort = addressPairs.get(firstHop);
        }

        if (addrPort == null) {
            return null;
        }

        String[] parts = addrPort.split(":");

        if (parts.length != 2) {
            return null;
        }

        String addr = parts[0];
        int targetPort = Integer.parseInt(parts[1]);

        int txID = generateTransactionID();

        byte[] txIDBytes = new byte[2];
        txIDBytes[0] = (byte) (txID >> 8);
        txIDBytes[1] = (byte) (txID & 0xFF);

        String payloadMessage = message;

        if (!relayStack.isEmpty()) {
            payloadMessage = wrapRelayMessage(message, targetNodeName);
        }

        byte[] fullMessage = buildPacket(txIDBytes, payloadMessage);

        DatagramPacket packet = new DatagramPacket(
                fullMessage,
                fullMessage.length,
                InetAddress.getByName(addr),
                targetPort
        );

        boolean isRelayed = !firstHop.equals(targetNodeName);

        socket.send(packet);

        pendingRequests.put(
                txID,
                new PendingRequest(addr, targetPort, fullMessage, targetNodeName, isRelayed)
        );

        long deadline = System.currentTimeMillis() + RETRANSMIT_INTERVAL_MS * (MAX_RETRIES + 1);

        while (System.currentTimeMillis() < deadline) {
            handleIncomingMessages(SOCKET_TIMEOUT_MS);

            if (responseMap.containsKey(txID)) {
                pendingRequests.remove(txID);
                return responseMap.remove(txID);
            }
        }

        pendingRequests.remove(txID);
        return null;
    }

    // ===== Interface Methods =====

    // Checks whether another node is reachable and returns the correct node name
    public boolean isActive(String targetNodeName) throws Exception {
        handleIncomingMessages(SOCKET_TIMEOUT_MS);

        if (!addressPairs.containsKey(targetNodeName)) {
            discoverNodeAddress(targetNodeName);
        }

        if (!addressPairs.containsKey(targetNodeName)) {
            return false;
        }

        String response = sendRequestAndWait(targetNodeName, "G");

        if (response == null || response.length() < 1 || response.charAt(0) != 'H') {
            return false;
        }

        String content = response.substring(1);
        content = stripLeadingSpace(content);

        DecodeResult decoded = decodeString(content, 0);

        return decoded.value.equals(targetNodeName);
    }

    //Adds a node to the relay stack so future requests are sent through it
    public void pushRelay(String targetNodeName) throws Exception {
        if (!addressPairs.containsKey(targetNodeName)) {
            discoverNodeAddress(targetNodeName);
        }

        if (addressPairs.containsKey(targetNodeName)) {
            relayStack.push(targetNodeName);
        }
    }

    //Removes the top node from the relay stack 
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            relayStack.pop();
        }
    }

    //Checks whether a key exists 
    public boolean exists(String key) throws Exception {
        handleIncomingMessages(SOCKET_TIMEOUT_MS);

        if (dataPairs.containsKey(key) || addressPairs.containsKey(key)) {
            return true;
        }

        if (!hasKnownPeers()) {
            bootstrapNetwork();
        }

        byte[] keyHash = HashID.computeHashID(key);
        List<String> closest = getClosestNodes(keyHash, 3);

        for (String node : closest) {
            String response = sendRequestAndWait(node, "E " + encodeString(key));

            if (response != null && response.length() > 0) {
                char responseType = response.charAt(0);
                String body = response.length() > 1 ? stripLeadingSpace(response.substring(1)) : "";

                if (responseType == 'F' && body.length() > 0) {
                    char code = body.charAt(0);

                    if (code == 'Y') {
                        return true;
                    }

                    if (code == '?') {
                        sendRequestAndWait(node, "N " + bytesToHex(keyHash));
                    }
                }

                if (responseType == 'Y') {
                    return true;
                }
            }

            if (response != null && response.length() > 0 && response.charAt(0) == 'O') {
                handleNearestResponse(response.substring(1));
            }
        }

        return false;
    }

    //Reads the value of a key, returning null if the key doesn't exist or no response is received within the timeout.
    public String read(String key) throws Exception {
        handleIncomingMessages(SOCKET_TIMEOUT_MS);

        if (dataPairs.containsKey(key)) {
            return dataPairs.get(key);
        }

        if (addressPairs.containsKey(key)) {
            return addressPairs.get(key);
        }

        if (!hasKnownPeers()) {
            bootstrapNetwork();
        }

        byte[] keyHash = HashID.computeHashID(key);
        List<String> closest = getClosestNodes(keyHash, 3);

        for (String node : closest) {
            String response = sendRequestAndWait(node, "R " + encodeString(key));

            if (response != null && response.length() > 0) {
                char responseType = response.charAt(0);
                String body = response.length() > 1 ? stripLeadingSpace(response.substring(1)) : "";

                if (responseType == 'S' && body.length() > 0) {
                    char code = body.charAt(0);

                    if (code == 'Y') {
                        String valuePart = body.length() > 1 ? stripLeadingSpace(body.substring(1)) : "";
                        DecodeResult val = decodeString(valuePart, 0);
                        return val.value;
                    }

                    if (code == '?') {
                        sendRequestAndWait(node, "N " + bytesToHex(keyHash));
                    }
                }

                if (responseType == 'Y') {
                    String content = stripLeadingSpace(response.substring(1));
                    DecodeResult val = decodeString(content, 0);
                    return val.value;
                }
            }
        }

        closest = getClosestNodes(keyHash, 3);

        for (String node : closest) {
            String response = sendRequestAndWait(node, "R " + encodeString(key));

            if (response != null && response.length() > 0) {
                char responseType = response.charAt(0);
                String body = response.length() > 1 ? stripLeadingSpace(response.substring(1)) : "";

                if (responseType == 'S' && body.length() > 0 && body.charAt(0) == 'Y') {
                    String valuePart = body.length() > 1 ? stripLeadingSpace(body.substring(1)) : "";
                    DecodeResult val = decodeString(valuePart, 0);
                    return val.value;
                }

                if (responseType == 'Y') {
                    String content = stripLeadingSpace(response.substring(1));
                    DecodeResult val = decodeString(content, 0);
                    return val.value;
                }
            }
        }

        return null;
    }

    //Writes a key/value pair to the CRN network and returns true if the write is successful or false if no response is received within the timeout or all responses indicate failure.
    public boolean write(String key, String value) throws Exception {
        handleIncomingMessages(SOCKET_TIMEOUT_MS);

        if (!hasKnownPeers()) {
            bootstrapNetwork();
        }

        byte[] keyHash = HashID.computeHashID(key);
        List<String> closest = getClosestNodes(keyHash, 3);

        boolean success = false;

        for (String node : closest) {
            String response = sendRequestAndWait(
                    node,
                    "W " + encodeString(key) + encodeString(value)
            );

            if (response != null && response.length() > 0) {
                char responseType = response.charAt(0);
                String body = response.length() > 1 ? stripLeadingSpace(response.substring(1)) : "";

                if (responseType == 'X' && body.length() > 0) {
                    char code = body.charAt(0);

                    if (code == 'R' || code == 'A') {
                        success = true;
                    }

                    if (code == '?') {
                        sendRequestAndWait(node, "N " + bytesToHex(keyHash));
                    }
                }

                if (responseType == 'R' || responseType == 'A') {
                    success = true;
                }
            }
        }

        handleIncomingMessages(SOCKET_TIMEOUT_MS);
        return success;
    }

    //Performs an atomic compare-and-swap operation on a key and returns true if the swap is successful or false if no response is received within the timeout, all responses indicate failure, or any response indicates that the key doesn't exist.
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        handleIncomingMessages(SOCKET_TIMEOUT_MS);

        if (!hasKnownPeers()) {
            bootstrapNetwork();
        }

        byte[] keyHash = HashID.computeHashID(key);
        List<String> closest = getClosestNodes(keyHash, 3);

        boolean success = false;

        for (String node : closest) {
            String response = sendRequestAndWait(
                    node,
                    "C " + encodeString(key) + encodeString(currentValue) + encodeString(newValue)
            );

            if (response != null && response.length() > 0) {
                char responseType = response.charAt(0);
                String body = response.length() > 1 ? stripLeadingSpace(response.substring(1)) : "";

                if (responseType == 'D' && body.length() > 0) {
                    char code = body.charAt(0);

                    if (code == 'R' || code == 'A') {
                        success = true;
                    }

                    if (code == '?') {
                        sendRequestAndWait(node, "N " + bytesToHex(keyHash));
                    }
                }

                if (responseType == 'R' || responseType == 'A') {
                    success = true;
                }
            }
        }

        handleIncomingMessages(SOCKET_TIMEOUT_MS);
        return success;
    }

    //Checks whether the given IP address and port are already known
    private boolean addressAlreadyKnown(String addr, int port) {
    String addrPort = addr + ":" + port;

    for (String knownAddrPort : addressPairs.values()) {
        if (knownAddrPort.equals(addrPort)) {
            return true;
        }
    }

    return false;
    }

    //Sends a name request to an unknown peer that has contacted this node and records the address if a valid response is received.
    private void requestNameFromPeer(String addr, int port) {
        try {
            if (addressAlreadyKnown(addr, port)) {
                return;
            }

            int txID = generateTransactionID();

            byte[] txIDBytes = new byte[2];
            txIDBytes[0] = (byte) (txID >> 8);
            txIDBytes[1] = (byte) (txID & 0xFF);

            byte[] fullMsg = buildPacket(txIDBytes, "G");

            DatagramPacket pkt = new DatagramPacket(
                    fullMsg,
                    fullMsg.length,
                    InetAddress.getByName(addr),
                    port
            );

            socket.send(pkt);

            pendingRequests.put(
                    txID,
                    new PendingRequest(addr, port, fullMsg, null, false)
            );
        } catch (Exception e) {
      
        }
    }
}

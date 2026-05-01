import java.lang.reflect.Method;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

public class FeatureTest {
    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        try {
            System.out.println("========================================");
            System.out.println(" CRN Local Feature Test");
            System.out.println("========================================");

            runBasicNetworkTests();
            runRawMessageTests();
            runMalformedMessageTests();
            runDuplicateRequestTest();
            runRetransmissionTest();

            System.out.println();
            System.out.println("========================================");
            System.out.println(" Test Summary");
            System.out.println("========================================");
            System.out.println("Passed: " + passed);
            System.out.println("Failed: " + failed);

            if (failed == 0) {
                System.out.println("Overall result: ALL TESTS PASSED");
            } else {
                System.out.println("Overall result: SOME TESTS FAILED");
            }

            System.out.println("========================================");

            System.exit(failed == 0 ? 0 : 1);

        } catch (Exception e) {
            System.err.println("Fatal exception during LocalTest");
            e.printStackTrace(System.err);
            System.exit(2);
        }
    }

    // ============================================================
    // 1. Basic NodeInterface tests
    // ============================================================

    private static void runBasicNetworkTests() throws Exception {
        System.out.println();
        System.out.println("[SECTION] Basic multi-node network tests");

        Node n0 = createNode("N:test0", 20110);
        Node n1 = createNode("N:test1", 20111);
        Node n2 = createNode("N:test2", 20112);
        Node n3 = createNode("N:test3", 20113);

        bootstrapAllKnownPeers(n0, n1, n2, n3);

        Thread t1 = startNodeThread(n1, "N:test1");
        Thread t2 = startNodeThread(n2, "N:test2");
        Thread t3 = startNodeThread(n3, "N:test3");

        Thread.sleep(500);

        check("isActive N:test1", n0.isActive("N:test1"));
        check("isActive N:test2", n0.isActive("N:test2"));
        check("isActive N:test3", n0.isActive("N:test3"));

        String key = "D:feature-basic";
        String value = "Hello CRN";

        boolean writeOk = n0.write(key, value);
        check("write basic data", writeOk);

        String readBack = n0.read(key);
        checkEquals("read basic data", value, readBack);

        check("exists returns true for written key", n0.exists(key));
        check("exists returns false for missing key", !n0.exists("D:missing-key-xyz"));

        String casKey = "D:feature-cas";
        check("write CAS base value", n0.write(casKey, "old"));
        check("CAS succeeds with matching current value", n0.CAS(casKey, "old", "new"));
        checkEquals("CAS read-back is new value", "new", n0.read(casKey));
        check("CAS fails with non-matching current value", !n0.CAS(casKey, "old", "bad"));
        checkEquals("CAS failed update did not overwrite value", "new", n0.read(casKey));

        n0.pushRelay("N:test1");

        String relayKey = "D:feature-relay";
        boolean relayWrite = n0.write(relayKey, "relay value");

        n0.popRelay();

        check("relay write through N:test1", relayWrite);
        checkEquals("relay read-back", "relay value", n0.read(relayKey));

        System.out.println("[INFO] Basic network tests complete.");
    }

    private static Node createNode(String name, int port) throws Exception {
        Node n = new Node();
        n.setNodeName(name);
        n.openPort(port);
        n.addOwnAddress("127.0.0.1", port);
        return n;
    }

    private static Thread startNodeThread(Node node, String label) {
        Thread t = new Thread(() -> {
            try {
                node.handleIncomingMessages(0);
            } catch (Exception e) {
                System.err.println("Unhandled exception in node thread " + label);
                e.printStackTrace(System.err);
            }
        });

        t.setDaemon(true);
        t.start();

        return t;
    }

    private static void bootstrapAllKnownPeers(Node n0, Node n1, Node n2, Node n3) throws Exception {
        addPeer(n0, "N:test1", "127.0.0.1:20111");
        addPeer(n0, "N:test2", "127.0.0.1:20112");
        addPeer(n0, "N:test3", "127.0.0.1:20113");

        addPeer(n1, "N:test0", "127.0.0.1:20110");
        addPeer(n1, "N:test2", "127.0.0.1:20112");
        addPeer(n1, "N:test3", "127.0.0.1:20113");

        addPeer(n2, "N:test0", "127.0.0.1:20110");
        addPeer(n2, "N:test1", "127.0.0.1:20111");
        addPeer(n2, "N:test3", "127.0.0.1:20113");

        addPeer(n3, "N:test0", "127.0.0.1:20110");
        addPeer(n3, "N:test1", "127.0.0.1:20111");
        addPeer(n3, "N:test2", "127.0.0.1:20112");
    }

    /*
     * This uses reflection only for local testing.
     * Do not copy this into Node.java.
     */
    private static void addPeer(Node node, String peerName, String address) throws Exception {
        Method recordAddress = Node.class.getDeclaredMethod("recordAddress", String.class, String.class);
        recordAddress.setAccessible(true);
        recordAddress.invoke(node, peerName, address);
    }

    // ============================================================
    // 2. Raw RFC message-format tests
    // ============================================================

    private static void runRawMessageTests() throws Exception {
        System.out.println();
        System.out.println("[SECTION] Raw CRN message-format tests");

        Node n = createNode("N:raw", 20120);

        Thread t = startNodeThread(n, "N:raw");
        Thread.sleep(300);

        DatagramSocket s = new DatagramSocket();
        s.setSoTimeout(3000);

        String h = sendRawAndReceivePayload(s, 20120, (byte) 0x10, (byte) 0x11, "G");
        check("raw G receives H response", h.startsWith("H "));
        check("raw H contains node name", h.contains("N:raw"));

        String writeResponse = sendRawAndReceivePayload(
                s,
                20120,
                (byte) 0x10,
                (byte) 0x12,
                "W " + encodeString("D:raw-key") + encodeString("raw value")
        );

        check("raw W receives X response type", writeResponse.startsWith("X "));
        check("raw W response is add or replace", writeResponse.equals("X A") || writeResponse.equals("X R"));

        String readResponse = sendRawAndReceivePayload(
                s,
                20120,
                (byte) 0x10,
                (byte) 0x13,
                "R " + encodeString("D:raw-key")
        );

        check("raw R receives S response type", readResponse.startsWith("S "));
        check("raw R response contains Y code", readResponse.startsWith("S Y "));
        check("raw R response contains value", readResponse.contains("raw value"));

        String existsResponse = sendRawAndReceivePayload(
                s,
                20120,
                (byte) 0x10,
                (byte) 0x14,
                "E " + encodeString("D:raw-key")
        );

        checkEquals("raw E receives F Y", "F Y", existsResponse);

        String casResponse = sendRawAndReceivePayload(
                s,
                20120,
                (byte) 0x10,
                (byte) 0x15,
                "C " + encodeString("D:raw-key") + encodeString("raw value") + encodeString("cas value")
        );

        checkEquals("raw C receives D R when current value matches", "D R", casResponse);

        String readAfterCas = sendRawAndReceivePayload(
                s,
                20120,
                (byte) 0x10,
                (byte) 0x16,
                "R " + encodeString("D:raw-key")
        );

        check("raw R after CAS contains new value", readAfterCas.contains("cas value"));

        String nearestResponse = sendRawAndReceivePayload(
                s,
                20120,
                (byte) 0x10,
                (byte) 0x17,
                "N " + bytesToHex(HashID.computeHashID("D:raw-key"))
        );

        check("raw N receives O response", nearestResponse.startsWith("O "));

        s.close();
    }

    // ============================================================
    // 3. Malformed packet tests
    // ============================================================

    private static void runMalformedMessageTests() throws Exception {
        System.out.println();
        System.out.println("[SECTION] Malformed-message robustness tests");

        Node n = createNode("N:malformed", 20121);

        Thread t = startNodeThread(n, "N:malformed");
        Thread.sleep(300);

        DatagramSocket s = new DatagramSocket();

        byte[][] badPackets = new byte[][] {
                new byte[] {0x01},
                new byte[] {0x01, 0x02},
                buildRawPacket((byte) 0x01, (byte) 0x02, ""),
                buildRawPacket((byte) 0x01, (byte) 0x03, "R"),
                buildRawPacket((byte) 0x01, (byte) 0x04, "R 9 "),
                buildRawPacket((byte) 0x01, (byte) 0x05, "W 0 D:bad "),
                buildRawPacket((byte) 0x01, (byte) 0x06, "C 0 D:bad 0 old "),
                buildRawPacket((byte) 0x01, (byte) 0x07, "N not-a-hash"),
                buildRawPacket((byte) 0x01, (byte) 0x08, "Z nonsense")
        };

        for (byte[] bad : badPackets) {
            DatagramPacket p = new DatagramPacket(
                    bad,
                    bad.length,
                    InetAddress.getByName("127.0.0.1"),
                    20121
            );

            s.send(p);
        }

        Thread.sleep(500);

        DatagramSocket checkSocket = new DatagramSocket();
        checkSocket.setSoTimeout(3000);

        String response = sendRawAndReceivePayload(
                checkSocket,
                20121,
                (byte) 0x22,
                (byte) 0x22,
                "G"
        );

        check("node still responds after malformed packets", response.startsWith("H "));

        s.close();
        checkSocket.close();
    }

    // ============================================================
    // 4. Duplicate request tests
    // ============================================================

    private static void runDuplicateRequestTest() throws Exception {
        System.out.println();
        System.out.println("[SECTION] Duplicate request handling test");

        Node n = createNode("N:duplicate", 20122);

        Thread t = startNodeThread(n, "N:duplicate");
        Thread.sleep(300);

        DatagramSocket s = new DatagramSocket();
        s.setSoTimeout(3000);

        byte[] duplicateRequest = buildRawPacket((byte) 0x33, (byte) 0x44, "G");

        DatagramPacket first = new DatagramPacket(
                duplicateRequest,
                duplicateRequest.length,
                InetAddress.getByName("127.0.0.1"),
                20122
        );

        DatagramPacket second = new DatagramPacket(
                duplicateRequest,
                duplicateRequest.length,
                InetAddress.getByName("127.0.0.1"),
                20122
        );

        s.send(first);
		String firstResponse = receivePayloadForTx(s, (byte) 0x33, (byte) 0x44);

		s.send(second);
		String secondResponse = receivePayloadForTx(s, (byte) 0x33, (byte) 0x44);

        check("first duplicate request receives response", firstResponse.startsWith("H "));
        check("second duplicate request receives response", secondResponse.startsWith("H "));
        checkEquals("duplicate responses match", firstResponse, secondResponse);

        s.close();
    }

    // ============================================================
    // 5. Retransmission test using a fake UDP server
    // ============================================================

    private static void runRetransmissionTest() throws Exception {
        System.out.println();
        System.out.println("[SECTION] Retransmission behaviour test");

        FakeSilentServer fake = new FakeSilentServer(20129);

        Thread fakeThread = new Thread(fake);
        fakeThread.setDaemon(true);
        fakeThread.start();

        Thread.sleep(300);

        Node n = createNode("N:retransmit", 20128);
        addPeer(n, "N:silent", "127.0.0.1:20129");

        long start = System.currentTimeMillis();
        boolean active = n.isActive("N:silent");
        long elapsed = System.currentTimeMillis() - start;

        fake.stop();

        check("silent fake peer is not active", !active);

        int received = fake.getReceivedCount();

        check("fake server received at least 2 packets, showing retransmission", received >= 2);
        check("fake server received no more than 4 packets, showing retry limit", received <= 4);
        check("isActive waited long enough to allow retransmission", elapsed >= 5000);

        System.out.println("[INFO] Fake silent server received " + received + " packet(s).");
        System.out.println("[INFO] isActive elapsed time: " + elapsed + " ms.");
    }

    private static class FakeSilentServer implements Runnable {
        private final int port;
        private volatile boolean running = true;
        private volatile int receivedCount = 0;
        private DatagramSocket socket;

        FakeSilentServer(int port) {
            this.port = port;
        }

        public void run() {
            try {
                socket = new DatagramSocket(port);
                socket.setSoTimeout(500);

                while (running) {
                    try {
                        byte[] buf = new byte[2048];
                        DatagramPacket p = new DatagramPacket(buf, buf.length);
                        socket.receive(p);
                        receivedCount++;
                    } catch (SocketTimeoutException e) {
                        // Normal timeout. Keep looping.
                    }
                }
            } catch (Exception e) {
                if (running) {
                    System.err.println("FakeSilentServer error:");
                    e.printStackTrace(System.err);
                }
            } finally {
                if (socket != null) {
                    socket.close();
                }
            }
        }

        void stop() {
            running = false;

            if (socket != null) {
                socket.close();
            }
        }

        int getReceivedCount() {
            return receivedCount;
        }
    }

    // ============================================================
    // Raw packet helpers
    // ============================================================

    private static String sendRawAndReceivePayload(
			DatagramSocket s,
			int port,
			byte tx1,
			byte tx2,
			String message
	) throws Exception {
		byte[] request = buildRawPacket(tx1, tx2, message);

		DatagramPacket p = new DatagramPacket(
				request,
				request.length,
				InetAddress.getByName("127.0.0.1"),
				port
		);

		s.send(p);
		return receivePayloadForTx(s, tx1, tx2);
	}

	private static String receivePayloadForTx(DatagramSocket s, byte expectedTx1, byte expectedTx2) throws Exception {
		long deadline = System.currentTimeMillis() + 3000;

		while (System.currentTimeMillis() < deadline) {
			byte[] buf = new byte[65536];

			DatagramPacket response = new DatagramPacket(buf, buf.length);

			try {
				s.receive(response);
			} catch (SocketTimeoutException e) {
				break;
			}

			if (response.getLength() < 3) {
				continue;
			}

			byte tx1 = buf[0];
			byte tx2 = buf[1];

			int start = 2;

			if (buf[2] == 0x20) {
				start = 3;
			}

			String payload = new String(
					buf,
					start,
					response.getLength() - start,
					StandardCharsets.UTF_8
			);

			if (tx1 == expectedTx1 && tx2 == expectedTx2) {
				return payload;
			}

			System.out.println("[INFO] Ignored non-matching packet while waiting for tx "
					+ unsigned(expectedTx1) + "," + unsigned(expectedTx2)
					+ ": tx=" + unsigned(tx1) + "," + unsigned(tx2)
					+ " msg=[" + payload + "]");
		}

		return "";
	}

    private static String receivePayload(DatagramSocket s) throws Exception {
		byte[] buf = new byte[65536];

		DatagramPacket response = new DatagramPacket(buf, buf.length);
		s.receive(response);

		if (response.getLength() < 3) {
			return "";
		}

		int start = 2;

		if (buf[2] == 0x20) {
			start = 3;
		}

		return new String(
				buf,
				start,
				response.getLength() - start,
				StandardCharsets.UTF_8
		);
	}

	private static int unsigned(byte b) {
		return b & 0xFF;
	}

    private static byte[] buildRawPacket(byte tx1, byte tx2, String message) {
        byte[] payload = message.getBytes(StandardCharsets.UTF_8);
        byte[] packet = new byte[3 + payload.length];

        packet[0] = tx1;
        packet[1] = tx2;
        packet[2] = 0x20;

        System.arraycopy(payload, 0, packet, 3, payload.length);

        return packet;
    }

    private static String encodeString(String s) {
        int spaces = 0;

        for (char c : s.toCharArray()) {
            if (c == ' ') {
                spaces++;
            }
        }

        return spaces + " " + s + " ";
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();

        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }

        return sb.toString();
    }

    // ============================================================
    // Test assertion helpers
    // ============================================================

    private static void check(String name, boolean condition) {
        if (condition) {
            passed++;
            System.out.println("[PASS] " + name);
        } else {
            failed++;
            System.out.println("[FAIL] " + name);
        }
    }

    private static void checkEquals(String name, String expected, String actual) {
        if (expected == null && actual == null) {
            passed++;
            System.out.println("[PASS] " + name);
            return;
        }

        if (expected != null && expected.equals(actual)) {
            passed++;
            System.out.println("[PASS] " + name);
        } else {
            failed++;
            System.out.println("[FAIL] " + name);
            System.out.println("       expected: " + expected);
            System.out.println("       actual  : " + actual);
        }
    }
}
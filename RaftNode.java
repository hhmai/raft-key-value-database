import org.json.JSONObject;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.Random;

public class RaftNode {

    private static class Entry {
        String key;
        String value;

        private Entry(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    private HashMap<String, String> database = new HashMap<>();
    private ArrayList<Entry> log = new ArrayList<>();
    private String leader;

    public enum State {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    private static final String BROADCAST = "FFFF";
    private int port;
    private String id;
    private String[] others;
    private State state;
    private int currentTerm;
    private final long ELECTION_TIMEOUT_MAX = new Random().nextInt(500 - 400) + 400;
    private static final int HEARTBEAT_INTERVAL_MS = new Random().nextInt(100 - 50) + 50;;
    private long lastHeartbeat = System.currentTimeMillis() + 3000;
    private int votesReceived = 0;
    // TODO: add log and other required fields
    private DatagramSocket socket;

    public RaftNode(int port, String id, String[] others) throws IOException, InterruptedException {
        this.state = State.FOLLOWER;
        this.currentTerm = 0;
        this.port = port;
        this.id = id;
        this.others = others;
        this.leader = "FFFF";

        this.socket = new DatagramSocket(null);
        this.socket.bind(new InetSocketAddress("localhost", 0));
        System.out.printf("Replica %s starting up%n", this.id);
        JSONObject json = new JSONObject()
                .put("src", this.id)
                .put("dst", BROADCAST)
                .put("leader", this.leader)
                .put("type", "hello");
        this.send(json.toString());
        System.out.printf("Sent hello message: %s%n", json.toString());
        // TODO: initialize log and other required fields
    }

    /**
     * Sends a packet through the socket
     * @param message message to send
     * @throws IOException
     */
    public void send(String message) throws IOException {
        byte[] buffer = message.getBytes(StandardCharsets.UTF_8);
        InetAddress address = InetAddress.getByName("localhost");
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, address, this.port);
        this.socket.send(packet);
    }

    private void run() throws IOException {
        while (true) {
            if (state != State.LEADER && System.currentTimeMillis() - lastHeartbeat > ELECTION_TIMEOUT_MAX) {
                // am follower, no heartbeat from Leader, start new election
                state = State.CANDIDATE;
                currentTerm++;
                votesReceived = 1; // Vote for self
                JSONObject leaderJSON = new JSONObject()
                        .put("src", this.id)
                        .put("dst", BROADCAST)
                        .put("type", "RequestVote")
                        .put("MID", "LEADERMID")
                        .put("leader", this.leader)
                        .put("term", this.currentTerm);
                this.send(leaderJSON.toString());
            } else if (state == State.LEADER && System.currentTimeMillis() - lastHeartbeat > HEARTBEAT_INTERVAL_MS) {
                // am leader, need to send a heartbeat
                JSONObject heartbeat = new JSONObject()
                        .put("src", this.id)
                        .put("dst", BROADCAST)
                        .put("type", "AppendEntry")
                        .put("leader", this.id)
                        .put("MID", "LEADERMID")
                        .put("key", "")
                        .put("value", "")
                        .put("term", this.currentTerm);
                this.send(heartbeat.toString());
                lastHeartbeat = System.currentTimeMillis();
            }

            // handle incoming requests
            byte[] buffer = new byte[65535];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            this.socket.setSoTimeout(1);
            try {
                this.socket.receive(packet);
                String msg = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                JSONObject jsonMsg = new JSONObject(msg);
                //System.out.printf("Received message '%s'%n", msg);
                if (state.equals(State.LEADER)) {
                    this.leaderReceive(jsonMsg);
                } else if (state.equals(State.FOLLOWER)) {
                    this.followerReceive(jsonMsg);
                } else {
                    // candidate
                    this.candidateReceive(jsonMsg);
                }
            } catch (SocketTimeoutException ignored) {}
        }
    }

    private void candidateReceive(JSONObject jsonMsg) throws IOException {
        switch (jsonMsg.getString("type")) {
            case "AppendEntry":
                // if a candidate receives a heartbeat with higher term, that is the leader else if lower keep waiting
                if (jsonMsg.getInt("term") > currentTerm) {
                    this.state = State.FOLLOWER;
                    this.currentTerm = jsonMsg.getInt("term");
                    if (!Objects.equals(jsonMsg.getString("key"), "")) {
                        this.database.put(jsonMsg.getString("key"), jsonMsg.getString("value"));
                    }
                }
                break;
            case "RequestVote":
                // if a candidate receives a request vote with a higher term, then vote for it and become a follower
                if (jsonMsg.getInt("term") > currentTerm) {
                    this.state = State.FOLLOWER;
                    this.currentTerm = jsonMsg.getInt("term");
                    JSONObject vote = new JSONObject()
                            .put("src", this.id)
                            .put("dst", jsonMsg.getString("src"))
                            .put("type", "Vote")
                            .put("MID", "VoteMid")
                            .put("leader", this.leader)
                            .put("term", this.currentTerm);
                    this.send(vote.toString());
                }
                break;
            case "Vote":
                // if message is a vote, then increase vote received
                votesReceived++;
                if (votesReceived >= 2) {
                    this.state = State.LEADER;
                    this.leader = this.id;
                    currentTerm++;
                }
                break;
            default:
                JSONObject redirectJson = new JSONObject()
                        .put("src", this.id)
                        .put("dst", jsonMsg.getString("src"))
                        .put("leader", this.leader)
                        .put("type", "redirect")
                        .put("MID", jsonMsg.getString("MID"));
                this.send(redirectJson.toString());
                break;
        }
    }

    private void followerReceive(JSONObject jsonMsg) throws IOException {
        switch (jsonMsg.getString("type")) {
            case "put":
            case "get":
                // if follower receives put or get, redirect to current leader
                JSONObject redirectJson = new JSONObject()
                        .put("src", this.id)
                        .put("dst", jsonMsg.getString("src"))
                        .put("leader", this.leader)
                        .put("type", "redirect")
                        .put("MID", jsonMsg.getString("MID"));
                this.send(redirectJson.toString());
                break;
            case "AppendEntry":
                // if follower receives heartbeat and it has a higher current term, that is the new leader
                if (jsonMsg.getInt("term") > currentTerm) {
                    if (!Objects.equals(jsonMsg.get("entries"), "")) {
                        //this.database = jsonMsg.get("entries");
                        this.database.put(jsonMsg.getString("key"), jsonMsg.getString("value"));
                    }
                    this.leader = jsonMsg.getString("src");
                    this.currentTerm = jsonMsg.getInt("term");
                    this.lastHeartbeat = System.currentTimeMillis();
                } else if (jsonMsg.getInt("term") == currentTerm) {
                    if (!Objects.equals(jsonMsg.getString("key"), "")) {
                        this.database.put(jsonMsg.getString("key"), jsonMsg.getString("value"));
                    }
                    // if a follower receives a heartbeat and it has the same current term,
                    this.lastHeartbeat = System.currentTimeMillis();
                }
                break;
            case "RequestVote":
                // if a follower receives a request vote and it has a higher term, then vote for it only if it
                // did not vote for a candidate already
                if (jsonMsg.getInt("term") > currentTerm) {
                    this.currentTerm = jsonMsg.getInt("term");
                }
                JSONObject vote = new JSONObject()
                        .put("src", this.id)
                        .put("dst", jsonMsg.getString("src"))
                        .put("leader", this.leader)
                        .put("type", "Vote")
                        .put("MID", "VoteMID")
                        .put("term", this.currentTerm);
                this.send(vote.toString());
                break;
            default:
        }
    }

    private void leaderReceive(JSONObject jsonMsg) throws IOException {
        switch (jsonMsg.getString("type")) {
            case "put":
                JSONObject appendEntry = new JSONObject()
                        .put("src", this.id)
                        .put("dst", BROADCAST)
                        .put("type", "AppendEntry")
                        .put("leader", this.id)
                        .put("MID", "LEADERMID")
                        //.put("entries", this.database)
                        .put("key", jsonMsg.getString("key"))
                        .put("value", jsonMsg.getString("value"))
                        .put("term", this.currentTerm)
                        .put("commitIndex", this.log.size()+1);
                this.send(appendEntry.toString());
                this.database.put(jsonMsg.getString("key"), jsonMsg.getString("value"));
                JSONObject putJson = new JSONObject()
                    .put("src", this.id)
                    .put("dst", jsonMsg.getString("src"))
                    .put("leader", this.id)
                    .put("type", "ok")
                    .put("MID", jsonMsg.getString("MID"));
                this.send(putJson.toString());
                break;
            case "get":
                if (this.database.containsKey(jsonMsg.getString("key"))) {
                    JSONObject getJson = new JSONObject()
                            .put("src", this.id)
                            .put("dst", jsonMsg.getString("src"))
                            .put("leader", this.id)
                            .put("type", "ok")
                            .put("MID", jsonMsg.getString("MID"))
                            .put("value", this.database.get(jsonMsg.getString("key")));
                    this.send(getJson.toString());
                }
                break;
            case "AppendEntry":
                // if leader receives append entry with higher term then set that as leader and move to follower state
                if (jsonMsg.getInt("term") > currentTerm) {
                    this.state = State.FOLLOWER;
                    this.leader = jsonMsg.getString("src");
                    this.currentTerm = jsonMsg.getInt("term");
                    this.lastHeartbeat = System.currentTimeMillis();
                } else if (jsonMsg.getInt("term") == currentTerm) {
                    // if a leader receives a heartbeat and it has the same current term, increase leaders current term
                    // to stay leader for now
                    this.lastHeartbeat = System.currentTimeMillis();
                    this.currentTerm++;
                }
            default:
                // do nothing for now
        }
    }

    /**
     * Main method to run node
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        int port = Integer.parseInt(args[0]);
        String id = args[1];
        String[] others = new String[args.length - 2];
        System.arraycopy(args, 2, others, 0, others.length);
        RaftNode raftNode = new RaftNode(port, id, others);
        raftNode.run();
        //System.out.println(System.currentTimeMillis() - (System.currentTimeMillis() + 3000));
    }
}
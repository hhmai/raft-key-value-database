import org.json.JSONObject;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.Random;

public class RaftNode {

    /**
     * Class representing a single entry in the log
     */
    private static class Entry {
        String key;
        String value;

        private Entry(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    private HashMap<String, String> database = new HashMap<>(); //<Key, Value> pair
    private ArrayList<Entry> logEntry = new ArrayList<>(); // list of all entries in order received
    private String leader;

    /**
     * Node states
     */
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
    //private int commitIndex;
    private final long ELECTION_TIMEOUT_MAX = new Random().nextInt(600 - 500) + 500;
    private static final int HEARTBEAT_INTERVAL_MS = new Random().nextInt(400 - 300) + 300;
    private static final int FOLLOWER_REPLY_MAX = 650;
    private long lastHeartbeat = System.currentTimeMillis() + 3000;
    private long lastFollowerReply = System.currentTimeMillis() + 4000;
    private int votesReceived = 0;
    private DatagramSocket socket;

    /**
     * Represents a single node in the RAFT protocol
     * @param port
     * @param id
     * @param others
     * @throws IOException
     * @throws InterruptedException
     */
    public RaftNode(int port, String id, String[] others) throws IOException, InterruptedException {
        this.state = State.FOLLOWER;
        this.currentTerm = 0;
        //this.commitIndex = 0;
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

    /**
     * Runs the raft protocol
     * @throws IOException
     */
    private void run() throws IOException {
        while (true) {
            if (state == State.CANDIDATE && System.currentTimeMillis() - lastHeartbeat > ELECTION_TIMEOUT_MAX) {
                // am candidate, election timeout, no heartbeat from Leader, start new election
                startElection();
            }
            if (state == State.FOLLOWER && System.currentTimeMillis() - lastHeartbeat > ELECTION_TIMEOUT_MAX) {
                // am follower, no heartbeat from Leader, start new election
                startElection();
            } else if (state == State.LEADER && System.currentTimeMillis() - lastHeartbeat > HEARTBEAT_INTERVAL_MS) {
                // am leader, need to send a heartbeat
                sendHeartBeatJson();
            }
            if (state == State.LEADER && System.currentTimeMillis() - lastFollowerReply > FOLLOWER_REPLY_MAX) {
                // am leader, no reply from followers, possible minority?
                //System.out.printf("DNR FOLLOWER REPLIES FROM " + this.id);
                startElection();
            }

            // handle incoming requests
            byte[] buffer = new byte[65535];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            this.socket.setSoTimeout(1);
            try {
                this.socket.receive(packet);
                String msg = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                JSONObject jsonMsg = new JSONObject(msg);
                //System.out.printf("Received message '%s'%n\n", msg);
                //System.out.printf(this.id + " State = " + this.state + " Leader = " + this.leader
                  //      + " Term = " + this.currentTerm + " Votes = " + this.votesReceived +
                    //    " Commit = " + this.logEntry.size() +System.lineSeparator());
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

    /**
     * Starts the election process, turns into a candidate, increases term by 1, and votes for itself
     * @throws IOException
     */
    private void startElection() throws IOException {
        state = State.CANDIDATE;
        currentTerm++;
        votesReceived = 1; // Vote for self
        this.lastHeartbeat = System.currentTimeMillis();
        JSONObject leaderJSON = new JSONObject()
                .put("src", this.id)
                .put("dst", BROADCAST)
                .put("type", "RequestVote")
                .put("MID", "LEADERMID")
                .put("leader", this.leader)
                .put("term", this.currentTerm);
        this.send(leaderJSON.toString());
    }

    /**
     * Send an empty append entry message
     * @throws IOException
     */
    private void sendHeartBeatJson() throws IOException {
        JSONObject heartbeat = new JSONObject()
                .put("src", this.id)
                .put("dst", BROADCAST)
                .put("type", "AppendEntry")
                .put("leader", this.id)
                .put("MID", "LEADERMID")
                .put("key", "")
                .put("value", "")
                .put("term", this.currentTerm)
                .put("commitIndex", this.logEntry.size());
        this.send(heartbeat.toString());
        lastHeartbeat = System.currentTimeMillis();
    }

    /**
     * Receive parameters for candidates
     * @param jsonMsg
     * @throws IOException
     */
    private void candidateReceive(JSONObject jsonMsg) throws IOException {
        switch (jsonMsg.getString("type")) {
            case "AppendEntry":
                // if a candidate receives a heartbeat with higher or equal term, that is the leader else if lower keep waiting
                if (jsonMsg.getInt("term") >= currentTerm || jsonMsg.getInt("commitIndex") >= this.logEntry.size()) {
                    this.state = State.FOLLOWER;
                    this.leader = jsonMsg.getString("src");
                    this.currentTerm = jsonMsg.getInt("term");
                    this.lastHeartbeat = System.currentTimeMillis();
                    updateLog(jsonMsg);
                }
                break;
            case "RequestVote":
                // if a candidate receives a request vote with a higher term, then vote for it and become a follower
                if (jsonMsg.getInt("term") > currentTerm) {
                    this.state = State.FOLLOWER;
                    this.currentTerm = jsonMsg.getInt("term");
                    this.lastHeartbeat = System.currentTimeMillis();
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
                    currentTerm = currentTerm + 1;
                    this.lastFollowerReply = System.currentTimeMillis();
                    // am leader, need to send a heartbeat
                    sendHeartBeatJson();
                    this.votesReceived = 0;
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

    /**
     * Receive parameters for followers
     * @param jsonMsg
     * @throws IOException
     */
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
                // if follower receives append entry, and it has a higher or equal current term and commit index, that is the leader
                if (jsonMsg.getInt("term") >= currentTerm && jsonMsg.getInt("commitIndex") >= this.logEntry.size()) {
                    this.state = State.FOLLOWER;
                    this.votesReceived = 0;
                    this.leader = jsonMsg.getString("src");
                    this.currentTerm = jsonMsg.getInt("term");
                    this.lastHeartbeat = System.currentTimeMillis();

                    updateLog(jsonMsg);

                    // send reply
                    JSONObject replyToAppend = new JSONObject()
                            .put("src", this.id)
                            .put("dst", jsonMsg.getString("src"))
                            .put("leader", this.leader)
                            .put("type", "replyToAppend")
                            .put("MID", "replyToAppendMID");
                    this.send(replyToAppend.toString());
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
                        .put("term", this.currentTerm)
                        .put("commitIndex", this.logEntry.size());
                this.send(vote.toString());
                break;
            default:
        }
    }

    /**
     * Receive parameters for leaders
     * @param jsonMsg
     * @throws IOException
     */
    private void leaderReceive(JSONObject jsonMsg) throws IOException {
        switch (jsonMsg.getString("type")) {
            case "replyToAppend":
                this.lastFollowerReply = System.currentTimeMillis();
                break;
            case "rapidAppendEntry":
                for (int i = jsonMsg.getInt("commitIndex"); i < this.logEntry.size(); i++) {
                    // start from followers commit index up to leaders commit index
                    JSONObject appendEntry = new JSONObject()
                            .put("src", this.id)
                            .put("dst", jsonMsg.getString("src"))
                            .put("type", "AppendEntry")
                            .put("leader", this.id)
                            .put("MID", "LEADERMID")
                            .put("key", this.logEntry.get(i).key)
                            .put("value", this.logEntry.get(i).value)
                            .put("term", this.currentTerm)
                            .put("commitIndex", this.logEntry.size());
                    this.send(appendEntry.toString());
                }
                this.lastFollowerReply = System.currentTimeMillis();
                break;
            case "put":
                //this.commitIndex++;
                this.logEntry.add(new Entry( jsonMsg.getString("key"), jsonMsg.getString("value"))); // add to log
                this.database.put(jsonMsg.getString("key"), jsonMsg.getString("value"));
                JSONObject appendEntry = new JSONObject()
                        .put("src", this.id)
                        .put("dst", BROADCAST)
                        .put("type", "AppendEntry")
                        .put("leader", this.id)
                        .put("MID", "LEADERMID")
                        .put("key", jsonMsg.getString("key"))
                        .put("value", jsonMsg.getString("value"))
                        .put("term", this.currentTerm)
                        .put("commitIndex", this.logEntry.size());
                this.send(appendEntry.toString());
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
                if (!Objects.equals(jsonMsg.get("key"), "")) {
                    this.database.put(jsonMsg.getString("key"), jsonMsg.getString("value"));
                }
                // if leader receives append entry with higher term and equal/higher commit index then set that as leader and move to follower state
                if (jsonMsg.getInt("term") > currentTerm && jsonMsg.getInt("commitIndex") >= this.logEntry.size()) {
                    this.state = State.FOLLOWER;
                    this.leader = jsonMsg.getString("src");
                    this.currentTerm = jsonMsg.getInt("term");
                    this.lastHeartbeat = System.currentTimeMillis();
                    updateLog(jsonMsg);
                } else if (jsonMsg.getInt("term") >= currentTerm && jsonMsg.getInt("commitIndex") < this.logEntry.size()) {
                    // if leader receives append entry with lower commit index and equal term, we should be
                    // the leader so increase our term
                    this.currentTerm = jsonMsg.getInt("term") + 1;
                } else if (jsonMsg.getInt("term") <= currentTerm && jsonMsg.getInt("commitIndex") > this.logEntry.size()) {
                    // if leader receives commit index that is higher than our commit index, that should be the leader
                    this.state = State.FOLLOWER;
                    this.leader = jsonMsg.getString("src");
                    this.currentTerm = jsonMsg.getInt("term");
                    this.lastHeartbeat = System.currentTimeMillis();
                    updateLog(jsonMsg);
                }
            default:
                // do nothing for now
        }
    }

    /**
     * Updates the RaftNode's log with the most up-to-date log entries according to commitIndex
     * @param jsonMsg
     * @throws IOException
     */
    private void updateLog(JSONObject jsonMsg) throws IOException {
        if (jsonMsg.getInt("commitIndex") > this.logEntry.size()) {
            // new commit received

            // add to our database
            if (!Objects.equals(jsonMsg.get("key"), "")) {
                this.database.put(jsonMsg.getString("key"), jsonMsg.getString("value"));
                this.logEntry.add(new Entry( jsonMsg.getString("key"), jsonMsg.getString("value")));
            }

            // we are behind a couple commits (more than 1 behind which is normal)
            if (jsonMsg.getInt("commitIndex") > this.logEntry.size()) {
                JSONObject rapidAppendEntry = new JSONObject()
                        .put("src", this.id)
                        .put("dst", this.leader)
                        .put("leader", this.leader)
                        .put("type", "rapidAppendEntry")
                        .put("MID", "FOLLOWERMID")
                        .put("term", this.currentTerm)
                        .put("commitIndex", this.logEntry.size()); // send our current index to get new logs
                this.send(rapidAppendEntry.toString());
            }
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
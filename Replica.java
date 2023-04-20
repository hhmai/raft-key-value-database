import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Objects;
import java.util.Timer;

import org.json.*;


public class Replica {
    private static final String BROADCAST = "FFFF";

    private int port;
    private String id;
    private String[] others;
    private DatagramSocket socket;

    private HashMap<String, String> database = new HashMap<>();


    /* Leader vars */
    private String leader = "0001";
    private int electionTimer = 4300;
    private int heartBeat = 4000;
    private int term = 0;
    private int votes = 0;


    public Replica(int port, String id, String[] others) throws IOException {
        this.port = port;
        this.id = id;
        this.others = others;

        this.socket = new DatagramSocket(null);
        this.socket.bind(new InetSocketAddress("localhost", 0));

        System.out.printf("Replica %s starting up%n", this.id);
        JSONObject json = new JSONObject()
                .put("src", this.id)
                .put("dst", BROADCAST)
                .put("leader", "0001")
                .put("type", "hello");
        //String hello = String.format("{\"src\": \"%s\", \"dst\": \"%s\", \"leader\": \"%s\", \"type\": \"hello\"}", this.id, BROADCAST, BROADCAST);
        this.send(json.toString());
        System.out.printf("Sent hello message: %s%n", json.toString());

        // should we send an election vote?
    }

    public void send(String message) throws IOException {
        byte[] buffer = message.getBytes(StandardCharsets.UTF_8);
        InetAddress address = InetAddress.getByName("localhost");
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, address, this.port);
        this.socket.send(packet);
    }

    public void run() throws IOException {
        while (true) {
            byte[] buffer = new byte[65535];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            this.socket.setSoTimeout(1);


            if (Objects.equals(leader, this.id) && heartBeat == 0) {
                // if LEADER then send heartbeats
                JSONObject heartbeat = new JSONObject()
                        .put("src", this.id)
                        .put("dst", BROADCAST)
                        .put("type", "AppendEntry")
                        .put("leader", this.id)
                        .put("MID", "LEADERMID")
                        .put("entry", "")
                        .put("term", this.term);
                this.send(heartbeat.toString());
                this.heartBeat = 100;
                this.electionTimer = 300;
            } else if (!Objects.equals(leader, this.id) && electionTimer == 0) {
                // if FOLLOWER and no heartbeats in a while, start election
                JSONObject leaderJSON = new JSONObject()
                        .put("src", this.id)
                        .put("dst", BROADCAST)
                        .put("type", "LEADERCONF")
                        .put("MID", "LEADERMID")
                        .put("leader", this.id)
                        .put("term", this.term);
                this.send(leaderJSON.toString());
                this.electionTimer = 300;
                this.heartBeat = 0;
                this.term++;
            }

            try {
                this.socket.receive(packet);
                String msg = new String(packet.getData(), 0, packet.getLength(), "utf-8");
                JSONObject jsonMsg = new JSONObject(msg);
                System.out.printf("Received message '%s'%n", msg);

                // leader actions
                if (Objects.equals(leader, this.id)) {
                    heartBeat--;
                    switch (jsonMsg.getString("type")) {
                        case "LEADERCONF":
                            this.leader = jsonMsg.getString("src");
                            this.term = jsonMsg.getInt("term");
                            this.electionTimer = 300;
                            break;
                        case "put":
                            this.database.put(jsonMsg.getString("key"), jsonMsg.getString("value"));
                            JSONObject putJson = new JSONObject()
                                    .put("src", this.id)
                                    .put("dst", jsonMsg.getString("src"))
                                    .put("leader", this.id)
                                    .put("type", "ok")
                                    .put("MID", jsonMsg.getString("MID"));
                            this.send(putJson.toString());
                            JSONObject appendEntry = new JSONObject()
                                    .put("src", this.id)
                                    .put("dst", BROADCAST)
                                    .put("type", "AppendEntry")
                                    .put("leader", this.id)
                                    .put("MID", "LEADERMID")
                                    .put("entry", "")
                                    .put("term", this.term);
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
                        default:
                            // do nothing
                    }
                } else {
                    electionTimer--;
                    // follower actions
                    switch (jsonMsg.getString("type")) {
                        case "LEADERCONF":
                            this.leader = jsonMsg.getString("src");
                            this.term = jsonMsg.getInt("term");
                            this.electionTimer = 300;
                            break;
                        case "RequestVote":
                            if (this.term == jsonMsg.getInt("term")) {
                                JSONObject voteJson = new JSONObject()
                                        .put("src", this.id)
                                        .put("dst", jsonMsg.getString("src"))
                                        .put("leader", jsonMsg.getString("src"))
                                        .put("type", "vote")
                                        .put("MID", jsonMsg.getString("VOTEMID"));
                                this.term++;
                                this.send(voteJson.toString());
                            }
                            break;
                        case "AppendEntry":
                            if (jsonMsg.getString("entry").equals("")) {
                                // heartbeat message
                                this.electionTimer = 300;
                                this.leader = jsonMsg.getString("leader");
                            }
                            break;
                        case "vote":
                            this.votes++;
                            if (this.votes >= 2) {
                                this.leader = this.id;
                            }
                            this.term = jsonMsg.getInt("term");
                            JSONObject leaderJSON = new JSONObject()
                                    .put("src", this.id)
                                    .put("dst", BROADCAST)
                                    .put("type", "LEADERCONF")
                                    .put("MID", "LEADERMID")
                                    .put("leader", this.id)
                                    .put("term", this.term);
                            this.send(leaderJSON.toString());
                            break;
                        default:
                            // follower actions
                            JSONObject redirectJson = new JSONObject()
                                    .put("src", this.id)
                                    .put("dst", jsonMsg.getString("src"))
                                    .put("leader", this.leader)
                                    .put("type", "redirect")
                                    .put("MID", jsonMsg.getString("MID"));
                            this.send(redirectJson.toString());
                    }
                }
            } catch (JSONException | SocketTimeoutException e) {
                if (Objects.equals(leader, this.id)) {
                    heartBeat--;
                } else {
                    electionTimer--;
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(args[0]);
        String id = args[1];
        String[] others = new String[args.length - 2];
        System.arraycopy(args, 2, others, 0, others.length);
        Replica replica = new Replica(port, id, others);
        replica.run();
    }
}

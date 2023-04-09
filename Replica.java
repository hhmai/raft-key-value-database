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
    private int electionTimer = 1000;
    private int heartBeat = 25;


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
                JSONObject heartbeat = new JSONObject()
                        .put("src", this.id)
                        .put("dst", BROADCAST)
                        .put("type", "AppendEntry")
                        .put("leader", "0001")
                        .put("MID", "LEADERMID")
                        .put("entries[]", "");
                this.send(heartbeat.toString());
                this.heartBeat = 200;
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
                        case "put":
                            this.database.put(jsonMsg.getString("key"), jsonMsg.getString("value"));
                            JSONObject putJson = new JSONObject()
                                    .put("src", this.id)
                                    .put("dst", jsonMsg.getString("src"))
                                    .put("leader", this.leader)
                                    .put("type", "ok")
                                    .put("MID", jsonMsg.getString("MID"));
                            this.send(putJson.toString());
                            break;
                        case "get":
                            if (this.database.containsKey(jsonMsg.getString("key"))) {
                                JSONObject getJson = new JSONObject()
                                        .put("src", this.id)
                                        .put("dst", jsonMsg.getString("src"))
                                        .put("leader", this.leader)
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

                        // follower actions
                        JSONObject redirectJson = new JSONObject()
                                .put("src", this.id)
                                .put("dst", jsonMsg.getString("src"))
                                .put("leader", this.leader)
                                .put("type", "redirect")
                                .put("MID", jsonMsg.getString("MID"));
                        this.send(redirectJson.toString());
                }
            } catch (JSONException | SocketTimeoutException e) {
                heartBeat--;
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

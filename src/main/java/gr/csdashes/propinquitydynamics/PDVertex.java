package gr.csdashes.propinquitydynamics;

import static gr.csdashes.propinquitydynamics.CalculationTable.calculateDD;
import static gr.csdashes.propinquitydynamics.CalculationTable.calculateII;
import static gr.csdashes.propinquitydynamics.CalculationTable.calculateRD;
import static gr.csdashes.propinquitydynamics.CalculationTable.calculateRI;
import static gr.csdashes.propinquitydynamics.CalculationTable.calculateRR;
import gr.csdashes.propinquitydynamics.io.MyTextArrayWritable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class PDVertex extends Vertex<Text, IntWritable, MapWritable> {

    Set<String> Nr = new HashSet<>(50); // The remaining neighboors
    Set<String> Ni = new HashSet<>(50); // The neighboors to be insterted
    Set<String> Nd = new HashSet<>(50); // The neighboors to be deleted
    // The propinquity value map
    Map<String, Integer> P = new HashMap<>(150);
    //cutting thresshold
    int a;
    //emerging value
    int b;

    private Step mainStep = new Step(2);
    private Step initializeStep = new Step(6);
    private Step incrementalStep = new Step(4);

    private int h(String a) {
        return Integer.valueOf(a);
    }

    private int h(Text a) {
        return h(a.toString());
    }

    /* Increase the propinquity for each of the list items.
     * @param vertexes The list of the vertex ids to increase the propinquity
     * @param operation The enum that identifies the operation (INCREASE OR
     * DECREASE)
     */
    private void updatePropinquity(Collection<String> vertexes, UpdatePropinquity operation) {
        switch (operation) {
            case INCREASE:
                for (String vertex : vertexes) {
                    if (this.P.containsKey(vertex)) {
                        P.put(vertex, P.get(vertex) + 1);
                    } else {
                        P.put(vertex, 1);
                    }
                }
                break;
            case DECREASE:
                for (String vertex : vertexes) {
                    if (this.P.containsKey(vertex)) {
                        P.put(vertex, P.get(vertex) - 1);
                    } else {
                        P.put(vertex, -1);
                    }
                }
                break;
        }
    }

    /* This method is responsible to initialize the propinquity
     * hash map in each vertex. Consists of 2 steps, the angle
     * and the conjugate propinquity update.
     * @param messages The messages received in each superstep.
     */
    private void initialize(Iterable<MapWritable> messages) throws IOException {
        switch (this.initializeStep.getStep()) {
            /* Create an undirected graph. From each vertex send
             * our vertex id to all of the neighboors.
             */
            case 0:
                MapWritable outMsg = new MapWritable();
                Text k = new Text("init");
                outMsg.put(k, this.getVertexID());
                this.sendMessageToNeighbors(outMsg);
                break;
            case 1:
                List<Edge<Text, IntWritable>> edges = this.getEdges();
                Set<String> uniqueEdges = new HashSet<>(50);
                for (Edge<Text, IntWritable> edge : edges) {
                    uniqueEdges.add(edge.getDestinationVertexID().toString());
                }
                k = new Text("init");
                for (MapWritable message : messages) {
                    Text id = (Text) message.get(k);
                    if (uniqueEdges.add(id.toString())) {
                        Edge<Text, IntWritable> e = new Edge<>(id, new IntWritable(0));
                        this.addEdge(e);
                    }
                }
                break;
            case 2:
                // Set Nr to our neighbors
                for (Edge<Text, IntWritable> edge : this.getEdges()) {
                    Nr.add(edge.getDestinationVertexID().toString());
                }

                /* The paper algorithm does not include the propinquity
                 * increase of the direct neighbours
                 */
                this.updatePropinquity(this.Nr, UpdatePropinquity.INCREASE);

                /* ==== Initialize angle propinquity ====
                 * The goal is to increase the propinquity between 2 
                 * vertexes according to the amoung of the common neighboors
                 * between them.
                 * Initialize the Nr Set by adding all the neighboors in
                 * it and send the Set to all neighboors so they know
                 * that for the vertexes of the Set, the sender vertex is
                 * a common neighboor.
                 */
                outMsg = new MapWritable();
                k = new Text("Nr");
                for (String v : this.Nr) {
                    outMsg.put(k, new MyTextArrayWritable(this.Nr, v));

                    this.sendMessage(new Text(v), outMsg);
                    outMsg = new MapWritable();
                }
                break;
            case 3:
                /* Initialize the propinquity hash map for the vertexes of the
                 * received list.
                 */
                k = new Text("Nr");
                for (MapWritable message : messages) {
                    List<String> commonNeighboors = Arrays.asList(((MyTextArrayWritable) message.get(k)).toStrings());
                    updatePropinquity(commonNeighboors,
                            UpdatePropinquity.INCREASE);
                }
                /* ==== Initialize conjugate propinquity ==== 
                 * The goal is to increase the propinquity of a vertex pair
                 * according to the amount of edges between the common neighboors
                 * of this pair.
                 * Send the neighboors list of the vertex to all his neighboors.
                 * To achive only one way communication, a function that compairs
                 * the vertex ids is being used.
                 */
                outMsg = new MapWritable();
                Integer id = Integer.parseInt(this.getVertexID().toString());
                for (String neighboor : this.Nr) {
                    if (Integer.parseInt(neighboor) > id) {
                        outMsg.put(k, new MyTextArrayWritable(this.Nr, neighboor));
                        this.sendMessage(new Text(neighboor), outMsg);
                    }
                }
                break;
            case 4:
                /* Find the intersection of the received vertex list and the
                 * neighboor list of the vertex so as to create a list of the
                 * common neighboors between the sender and the receiver vertex.
                 * Send the intersection list to every element of this list so
                 * as to increase the propinquity.
                 */
                List<String> Nr_neighboors;
                k = new Text("Intersection");
                Text kNr = new Text("Nr");
                for (MapWritable message : messages) {
                    Nr_neighboors = Arrays.asList(((MyTextArrayWritable) message.get(kNr)).toStrings());

                    boolean Nr1IsLarger = Nr.size() > Nr_neighboors.size();
                    Set<String> intersection = new HashSet<>(Nr1IsLarger ? Nr_neighboors : Nr);
                    intersection.retainAll(Nr1IsLarger ? Nr : Nr_neighboors);

                    for (String vertex : intersection) {
                        Set<String> messageList = new HashSet<>(intersection);
                        messageList.remove(vertex);

                        if (!messageList.isEmpty()) {
                            MyTextArrayWritable aw = new MyTextArrayWritable(messageList);
                            outMsg = new MapWritable();
                            outMsg.put(k, aw);
                            this.sendMessage(new Text(vertex), outMsg);
                        }
                    }
                }
                break;
            case 5:
                // update the conjugate propinquity
                k = new Text("Intersection");
                for (MapWritable message : messages) {
                    MyTextArrayWritable incoming = (MyTextArrayWritable) message.get(k);
                    Nr_neighboors = Arrays.asList(incoming.toStrings());

                    updatePropinquity(Nr_neighboors,
                            UpdatePropinquity.INCREASE);
                }
                this.mainStep.increaseStep();
                break;
        }

        this.initializeStep.increaseStep();
        /* ==== Initialize conjugate propinquity end ==== */
    }

    /* This method is responsible for the incremental update
     * @param messages The messages received in each superstep.
     */
    private void incremental(Iterable<MapWritable> messages) throws IOException {

        switch (this.incrementalStep.getStep()) {
            case 0:
                int terminateCond = 0;
                this.Ni.clear();
                this.Nd.clear();

                for (Map.Entry<String, Integer> entry : this.P.entrySet()) {
                    if (entry.getValue() <= this.a && this.Nr.contains(entry.getKey())) {
                        this.Nd.add(entry.getKey());
                        this.Nr.remove(entry.getKey());
                        terminateCond++;
                    } else if (entry.getValue() >= this.b && !this.Nr.contains(entry.getKey())) {
                        this.Ni.add(entry.getKey());
                        terminateCond++;
                    }
                }

                // If we don't have any additions on Ni and Nd, we can vote to
                // halt.
                if (terminateCond == 0) {
//                    redistributeEdges();
                    break;
                }

                // We take care of the direct connections here. If we delete a neightbor,
                // we must decrease the propinquity etc...
                this.updatePropinquity(this.Ni, UpdatePropinquity.INCREASE);
                this.updatePropinquity(this.Nd, UpdatePropinquity.DECREASE);

                Text incr = new Text("PU+");
                Text decr = new Text("PU-");
                for (String vertex : this.Nr) {
                    Text target = new Text(vertex);
                    MapWritable outMsg = new MapWritable();

                    outMsg.put(incr, new MyTextArrayWritable(this.Ni));
                    this.sendMessage(target, outMsg);

                    outMsg = new MapWritable();

                    outMsg.put(decr, new MyTextArrayWritable(this.Nd));
                    this.sendMessage(target, outMsg);
                }
                for (String vertex : this.Ni) {
                    Text target = new Text(vertex);
                    MapWritable outMsg = new MapWritable();

                    outMsg.put(incr, new MyTextArrayWritable(this.Nr));
                    this.sendMessage(target, outMsg);

                    outMsg = new MapWritable();

                    outMsg.put(incr, new MyTextArrayWritable(this.Ni, vertex));
                    this.sendMessage(target, outMsg);
                }
                for (String vertex : this.Nd) {
                    Text target = new Text(vertex);
                    MapWritable outMsg = new MapWritable();

                    outMsg.put(decr, new MyTextArrayWritable(this.Nr));
                    this.sendMessage(target, outMsg);

                    outMsg = new MapWritable();

                    outMsg.put(decr, new MyTextArrayWritable(this.Nd, vertex));
                    this.sendMessage(target, outMsg);
                }
                break;
            case 1:
                incr = new Text("PU+");
                decr = new Text("PU-");
                for (MapWritable message : messages) {
                    if (message.containsKey(incr)) {
                        String[] s = ((MyTextArrayWritable) message.get(incr)).toStrings();

                        updatePropinquity(Arrays.asList(s), UpdatePropinquity.INCREASE);
                    } else if (message.containsKey(decr)) {
                        String[] s = ((MyTextArrayWritable) message.get(decr)).toStrings();

                        updatePropinquity(Arrays.asList(s), UpdatePropinquity.DECREASE);
                    }
                }

                Text sender = new Text("Sender");
                Text dnNr = new Text("DN NR");
                Text dnNi = new Text("DN NI");
                Text dnNd = new Text("DN ND");
                for (String vertex : this.Nr) {
                    if (h(vertex) > h(this.getVertexID())) {
                        MapWritable outMsg = new MapWritable();

                        outMsg.put(sender, this.getVertexID());
                        outMsg.put(dnNr, new MyTextArrayWritable(this.Nr));
                        outMsg.put(dnNi, new MyTextArrayWritable(this.Ni));
                        outMsg.put(dnNd, new MyTextArrayWritable(this.Nd));
                        this.sendMessage(new Text(vertex), outMsg);
                    }
                }
                for (String vertex : this.Ni) {
                    if (h(vertex) > h(this.getVertexID())) {
                        MapWritable outMsg = new MapWritable();

                        outMsg.put(sender, this.getVertexID());
                        outMsg.put(dnNr, new MyTextArrayWritable(this.Nr));
                        outMsg.put(dnNi, new MyTextArrayWritable(this.Ni));
                        this.sendMessage(new Text(vertex), outMsg);
                    }
                }
                for (String vertex : this.Nd) {
                    if (h(vertex) > h(this.getVertexID())) {
                        MapWritable outMsg = new MapWritable();

                        outMsg.put(sender, this.getVertexID());
                        outMsg.put(dnNr, new MyTextArrayWritable(this.Nr));
                        outMsg.put(dnNd, new MyTextArrayWritable(this.Nd));
                        this.sendMessage(new Text(vertex), outMsg);
                    }
                }
                break;
            case 2:
                incr = new Text("PU+");
                decr = new Text("PU-");

                sender = new Text("Sender");
                dnNr = new Text("DN NR");
                dnNi = new Text("DN NI");
                dnNd = new Text("DN ND");

                for (MapWritable message : messages) {
                    String senderId = ((Text) message.get(sender)).toString();

                    MyTextArrayWritable messageValueNr = (MyTextArrayWritable) message.get(dnNr);
                    MyTextArrayWritable messageValueNi = (MyTextArrayWritable) message.get(dnNi);
                    MyTextArrayWritable messageValueNd = (MyTextArrayWritable) message.get(dnNd);

                    if (messageValueNi == null) {
                        messageValueNi = new MyTextArrayWritable(new String[0]);
                    }
                    if (messageValueNd == null) {
                        messageValueNd = new MyTextArrayWritable(new String[0]);
                    }

                    if (this.Nr.contains(senderId)) {
                        //calculate RR
                        String[] RRList = calculateRR(this.Nr,
                                new HashSet<>(Arrays.asList(messageValueNr.toStrings())));
                        //calculate RI
                        String[] RIList = calculateRI(this.Nr, this.Ni,
                                new HashSet<>(Arrays.asList(messageValueNr.toStrings())),
                                new HashSet<>(Arrays.asList(messageValueNi.toStrings())));
                        //calculate RD
                        String[] RDList = calculateRD(this.Nr, this.Nd,
                                new HashSet<>(Arrays.asList(messageValueNr.toStrings())),
                                new HashSet<>(Arrays.asList(messageValueNd.toStrings())));

                        for (String vertex : RRList) {
                            MapWritable outMsg = new MapWritable();

                            outMsg.put(incr, new MyTextArrayWritable(RIList));
                            this.sendMessage(new Text(vertex), outMsg);

                            outMsg = new MapWritable();

                            outMsg.put(decr, new MyTextArrayWritable(RDList));
                            this.sendMessage(new Text(vertex), outMsg);
                        }
                        for (String vertex : RIList) {
                            MapWritable outMsg = new MapWritable();

                            outMsg.put(incr, new MyTextArrayWritable(RRList));
                            this.sendMessage(new Text(vertex), outMsg);

                            outMsg = new MapWritable();

                            outMsg.put(incr, new MyTextArrayWritable(RIList, vertex));
                            this.sendMessage(new Text(vertex), outMsg);
                        }
                        for (String vertex : RDList) {
                            MapWritable outMsg = new MapWritable();

                            outMsg.put(decr, new MyTextArrayWritable(RRList));
                            this.sendMessage(new Text(vertex), outMsg);

                            outMsg = new MapWritable();

                            outMsg.put(decr, new MyTextArrayWritable(RDList, vertex));
                            this.sendMessage(new Text(vertex), outMsg);
                        }
                    }
                    if (this.Ni.contains(senderId)) {
                        //calculate II
                        String[] IIList = calculateII(this.Nr, this.Ni,
                                new HashSet<>(Arrays.asList(messageValueNr.toStrings())),
                                new HashSet<>(Arrays.asList(messageValueNi.toStrings())));
                        for (String vertex : IIList) {
                            MapWritable outMsg = new MapWritable();

                            outMsg.put(incr, new MyTextArrayWritable(IIList, vertex));
                            this.sendMessage(new Text(vertex), outMsg);
                        }
                    }
                    if (this.Nd.contains(senderId)) {
                        //calculate DD
                        String[] DDList = calculateDD(this.Nr, this.Nd,
                                new HashSet<>(Arrays.asList(messageValueNr.toStrings())),
                                new HashSet<>(Arrays.asList(messageValueNd.toStrings())));
                        for (String vertex : DDList) {
                            MapWritable outMsg = new MapWritable();

                            outMsg.put(decr, new MyTextArrayWritable(DDList, vertex));
                            this.sendMessage(new Text(vertex), outMsg);
                        }
                    }
                }
                break;
            case 3:
                incr = new Text("PU+");
                decr = new Text("PU-");
                for (MapWritable message : messages) {
                    if (message.containsKey(incr)) {
                        String[] s = ((MyTextArrayWritable) message.get(incr)).toStrings();

                        updatePropinquity(Arrays.asList(s), UpdatePropinquity.INCREASE);
                    } else if (message.containsKey(decr)) {
                        String[] s = ((MyTextArrayWritable) message.get(decr)).toStrings();

                        updatePropinquity(Arrays.asList(s), UpdatePropinquity.DECREASE);
                    }
                }

                // NOT! NR ← NR + ND
                // BUT! NR ← NR + NI
                this.Nr.addAll(this.Ni);
                break;
        }

        this.incrementalStep.increaseStep();
    }

    private void redistributeEdges() {
        this.setEdges(null);
        for (String e : this.Nr) {
            this.addEdge(new Edge<>(new Text(e), new IntWritable(this.P.get(e))));
        }
        voteToHalt();
    }

    @Override
    public void compute(Iterable<MapWritable> messages) throws IOException {
        this.a = this.getConf().getInt("a", 0);
        this.b = this.getConf().getInt("b", 1000);

        if (this.getSuperstepCount() > 0) {
            this.deserialize();
        }

        switch (this.mainStep.getStep()) {
            case 0:
                initialize(messages);
                break;
            case 1:
                incremental(messages);
                break;
        }

        if (this.getSuperstepCount() >= 100) {
            redistributeEdges();
        }

        this.serialize();
    }

    private void deserialize() {
        Text k = new Text("Nd");
        MyTextArrayWritable aw = (MyTextArrayWritable) this.getValue().get(k);
        for (Writable t : aw.get()) {
            String s = ((Text) t).toString();
            this.Nd.add(s);
        }
        k.set("Ni");
        aw = (MyTextArrayWritable) this.getValue().get(k);
        for (Writable t : aw.get()) {
            String s = ((Text) t).toString();
            this.Ni.add(s);
        }
        k.set("Nr");
        aw = (MyTextArrayWritable) this.getValue().get(k);
        for (Writable t : aw.get()) {
            String s = ((Text) t).toString();
            this.Nr.add(s);
        }

        k.set("P");
        MapWritable m = (MapWritable) this.getValue().get(k);
        for (Entry<Writable, Writable> e : m.entrySet()) {
            String key = ((Text) e.getKey()).toString();
            Integer value = ((IntWritable) e.getValue()).get();
            this.P.put(key, value);
        }

        k.set("incrementalStep");
        this.incrementalStep = (Step) this.getValue().get(k);
        k.set("initializeStep");
        this.initializeStep = (Step) this.getValue().get(k);
        k.set("mainStep");
        this.mainStep = (Step) this.getValue().get(k);
    }

    private void serialize() {
        MapWritable valueMap = new MapWritable();

        MapWritable Pmap = new MapWritable();
        for (Entry<String, Integer> e : this.P.entrySet()) {
            Pmap.put(new Text(e.getKey()), new IntWritable(e.getValue()));
        }
        valueMap.put(new Text("P"), Pmap);

        valueMap.put(new Text("Nr"), new MyTextArrayWritable(this.Nr));
        valueMap.put(new Text("Ni"), new MyTextArrayWritable(this.Ni));
        valueMap.put(new Text("Nd"), new MyTextArrayWritable(this.Nd));

        valueMap.put(new Text("incrementalStep"), this.incrementalStep);
        valueMap.put(new Text("initializeStep"), this.initializeStep);
        valueMap.put(new Text("mainStep"), this.mainStep);

        this.setValue(valueMap);
    }
}

package gr.csdashes.propinquitydynamicsnostrings;

import static gr.csdashes.propinquitydynamicsnostrings.CalculationTable.calculateDD;
import static gr.csdashes.propinquitydynamicsnostrings.CalculationTable.calculateII;
import static gr.csdashes.propinquitydynamicsnostrings.CalculationTable.calculateRD;
import static gr.csdashes.propinquitydynamicsnostrings.CalculationTable.calculateRI;
import static gr.csdashes.propinquitydynamicsnostrings.CalculationTable.calculateRR;
import gr.csdashes.propinquitydynamicsnostrings.io.MyVIntArrayWritable;
import java.io.IOException;
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
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class PDVertex extends Vertex<VIntWritable, VIntWritable, MapWritable> {

    Set<Integer> Nr = new HashSet<>(50); // The remaining neighboors
    Set<Integer> Ni = new HashSet<>(50); // The neighboors to be insterted
    Set<Integer> Nd = new HashSet<>(50); // The neighboors to be deleted
    // The propinquity value map
    Map<Integer, Integer> P = new HashMap<>(150);
    //cutting thresshold
    int a;
    //emerging value
    int b;

    private Step mainStep = new Step(2);
    private Step initializeStep = new Step(6);
    private Step incrementalStep = new Step(4);

    /* Increase the propinquity for each of the list items.
     * @param vertexes The list of the vertex ids to increase the propinquity
     * @param operation The enum that identifies the operation (INCREASE OR
     * DECREASE)
     */
    private void updatePropinquity(Collection<Integer> vertexes, UpdatePropinquity operation) {
        switch (operation) {
            case INCREASE:
                for (Integer vertex : vertexes) {
                    if (this.P.containsKey(vertex)) {
                        P.put(vertex, P.get(vertex) + 1);
                    } else {
                        P.put(vertex, 1);
                    }
                }
                break;
            case DECREASE:
                for (Integer vertex : vertexes) {
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
                List<Edge<VIntWritable, VIntWritable>> edges = this.getEdges();
                Set<Integer> uniqueEdges = new HashSet<>(50);
                for (Edge<VIntWritable, VIntWritable> edge : edges) {
                    uniqueEdges.add(edge.getDestinationVertexID().get());
                }
                k = new Text("init");
                for (MapWritable message : messages) {
                    VIntWritable id = (VIntWritable) message.get(k);
                    if (uniqueEdges.add(id.get())) {
                        Edge<VIntWritable, VIntWritable> e = new Edge<>(id, new VIntWritable(0));
                        this.addEdge(e);
                    }
                }
                break;
            case 2:
                // Set Nr to our neighbors
                for (Edge<VIntWritable, VIntWritable> edge : this.getEdges()) {
                    Nr.add(edge.getDestinationVertexID().get());
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
                for (Integer v : this.Nr) {
                    outMsg.put(k, new MyVIntArrayWritable(this.Nr, v));

                    this.sendMessage(new VIntWritable(v), outMsg);
                    outMsg = new MapWritable(); // TODO: Test if we can outMsg.clear()
                }
                break;
            case 3:
                /* Initialize the propinquity hash map for the vertexes of the
                 * received list.
                 */
                k = new Text("Nr");
                for (MapWritable message : messages) {
                    List<Integer> commonNeighboors = ((MyVIntArrayWritable) message.get(k)).toList();
                    updatePropinquity(commonNeighboors, UpdatePropinquity.INCREASE);
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
                Integer id = this.getVertexID().get();
                for (Integer neighboor : this.Nr) {
                    if (neighboor > id) {
                        outMsg.put(k, new MyVIntArrayWritable(this.Nr, neighboor));
                        this.sendMessage(new VIntWritable(neighboor), outMsg);
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
                List<Integer> Nr_neighboors;
                k = new Text("Intersection");
                Text kNr = new Text("Nr");
                for (MapWritable message : messages) {
                    Nr_neighboors = ((MyVIntArrayWritable) message.get(kNr)).toList();

                    boolean Nr1IsLarger = Nr.size() > Nr_neighboors.size();
                    Set<Integer> intersection = new HashSet<>(Nr1IsLarger ? Nr_neighboors : Nr);
                    intersection.retainAll(Nr1IsLarger ? Nr : Nr_neighboors);

                    for (Integer vertex : intersection) {
                        Set<Integer> messageList = new HashSet<>(intersection);
                        messageList.remove(vertex);

                        if (!messageList.isEmpty()) {
                            MyVIntArrayWritable aw = new MyVIntArrayWritable(messageList);
                            outMsg = new MapWritable();
                            outMsg.put(k, aw);
                            this.sendMessage(new VIntWritable(vertex), outMsg);
                        }
                    }
                }
                break;
            case 5:
                // update the conjugate propinquity
                k = new Text("Intersection");
                for (MapWritable message : messages) {
                    MyVIntArrayWritable incoming = (MyVIntArrayWritable) message.get(k);
                    Nr_neighboors = incoming.toList();

                    updatePropinquity(Nr_neighboors, UpdatePropinquity.INCREASE);
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

                for (Map.Entry<Integer, Integer> entry : this.P.entrySet()) {
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
//                if (terminateCond == 0) {
//                    redistributeEdges();
//                    break;
//                }

                // We take care of the direct connections here. If we delete a neightbor,
                // we must decrease the propinquity etc...
                this.updatePropinquity(this.Ni, UpdatePropinquity.INCREASE);
                this.updatePropinquity(this.Nd, UpdatePropinquity.DECREASE);

                Text incr = new Text("PU+");
                Text decr = new Text("PU-");
                for (Integer vertex : this.Nr) {
                    VIntWritable target = new VIntWritable(vertex);
                    MapWritable outMsg = new MapWritable();

                    outMsg.put(incr, new MyVIntArrayWritable(this.Ni));
                    this.sendMessage(target, outMsg);

                    outMsg = new MapWritable();

                    outMsg.put(decr, new MyVIntArrayWritable(this.Nd));
                    this.sendMessage(target, outMsg);
                }
                for (Integer vertex : this.Ni) {
                    VIntWritable target = new VIntWritable(vertex);
                    MapWritable outMsg = new MapWritable();

                    outMsg.put(incr, new MyVIntArrayWritable(this.Nr));
                    this.sendMessage(target, outMsg);

                    outMsg = new MapWritable();

                    outMsg.put(incr, new MyVIntArrayWritable(this.Ni, vertex));
                    this.sendMessage(target, outMsg);
                }
                for (Integer vertex : this.Nd) {
                    VIntWritable target = new VIntWritable(vertex);
                    MapWritable outMsg = new MapWritable();

                    outMsg.put(decr, new MyVIntArrayWritable(this.Nr));
                    this.sendMessage(target, outMsg);

                    outMsg = new MapWritable();

                    outMsg.put(decr, new MyVIntArrayWritable(this.Nd, vertex));
                    this.sendMessage(target, outMsg);
                }
                break;
            case 1:
                incr = new Text("PU+");
                decr = new Text("PU-");
                for (MapWritable message : messages) {
                    if (message.containsKey(incr)) {
                        List<Integer> s = ((MyVIntArrayWritable) message.get(incr)).toList();

                        updatePropinquity(s, UpdatePropinquity.INCREASE);
                    } else if (message.containsKey(decr)) {
                        List<Integer> s = ((MyVIntArrayWritable) message.get(decr)).toList();

                        updatePropinquity(s, UpdatePropinquity.DECREASE);
                    }
                }

                Text sender = new Text("Sender");
                Text dnNr = new Text("DN NR");
                Text dnNi = new Text("DN NI");
                Text dnNd = new Text("DN ND");
                for (Integer vertex : this.Nr) {
                    if ((vertex > this.getVertexID().get())) {
                        MapWritable outMsg = new MapWritable();

                        outMsg.put(sender, this.getVertexID());
                        outMsg.put(dnNr, new MyVIntArrayWritable(this.Nr));
                        outMsg.put(dnNi, new MyVIntArrayWritable(this.Ni));
                        outMsg.put(dnNd, new MyVIntArrayWritable(this.Nd));
                        this.sendMessage(new VIntWritable(vertex), outMsg);
                    }
                }
                for (Integer vertex : this.Ni) {
                    if (vertex > this.getVertexID().get()) {
                        MapWritable outMsg = new MapWritable();

                        outMsg.put(sender, this.getVertexID());
                        outMsg.put(dnNr, new MyVIntArrayWritable(this.Nr));
                        outMsg.put(dnNi, new MyVIntArrayWritable(this.Ni));
                        this.sendMessage(new VIntWritable(vertex), outMsg);
                    }
                }
                for (Integer vertex : this.Nd) {
                    if (vertex > this.getVertexID().get()) {
                        MapWritable outMsg = new MapWritable();

                        outMsg.put(sender, this.getVertexID());
                        outMsg.put(dnNr, new MyVIntArrayWritable(this.Nr));
                        outMsg.put(dnNd, new MyVIntArrayWritable(this.Nd));
                        this.sendMessage(new VIntWritable(vertex), outMsg);
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
                    Integer senderId = ((VIntWritable) message.get(sender)).get();

                    MyVIntArrayWritable messageValueNr = (MyVIntArrayWritable) message.get(dnNr);
                    MyVIntArrayWritable messageValueNi = (MyVIntArrayWritable) message.get(dnNi);
                    MyVIntArrayWritable messageValueNd = (MyVIntArrayWritable) message.get(dnNd);

                    if (messageValueNi == null) {
                        messageValueNi = new MyVIntArrayWritable(new Integer[0]);
                    }
                    if (messageValueNd == null) {
                        messageValueNd = new MyVIntArrayWritable(new Integer[0]);
                    }

                    if (this.Nr.contains(senderId)) {
                        //calculate RR
                        Integer[] RRList = calculateRR(this.Nr,
                                new HashSet<>(messageValueNr.toList()));
                        //calculate RI
                        Integer[] RIList = calculateRI(this.Nr, this.Ni,
                                new HashSet<>(messageValueNr.toList()),
                                new HashSet<>(messageValueNi.toList()));
                        //calculate RD
                        Integer[] RDList = calculateRD(this.Nr, this.Nd,
                                new HashSet<>(messageValueNr.toList()),
                                new HashSet<>(messageValueNd.toList()));

                        for (Integer vertex : RRList) {
                            MapWritable outMsg = new MapWritable();

                            outMsg.put(incr, new MyVIntArrayWritable(RIList));
                            this.sendMessage(new VIntWritable(vertex), outMsg);

                            outMsg = new MapWritable();

                            outMsg.put(decr, new MyVIntArrayWritable(RDList));
                            this.sendMessage(new VIntWritable(vertex), outMsg);
                        }
                        for (Integer vertex : RIList) {
                            MapWritable outMsg = new MapWritable();

                            outMsg.put(incr, new MyVIntArrayWritable(RRList));
                            this.sendMessage(new VIntWritable(vertex), outMsg);

                            outMsg = new MapWritable();

                            outMsg.put(incr, new MyVIntArrayWritable(RIList, vertex));
                            this.sendMessage(new VIntWritable(vertex), outMsg);
                        }
                        for (Integer vertex : RDList) {
                            MapWritable outMsg = new MapWritable();

                            outMsg.put(decr, new MyVIntArrayWritable(RRList));
                            this.sendMessage(new VIntWritable(vertex), outMsg);

                            outMsg = new MapWritable();

                            outMsg.put(decr, new MyVIntArrayWritable(RDList, vertex));
                            this.sendMessage(new VIntWritable(vertex), outMsg);
                        }
                    }
                    if (this.Ni.contains(senderId)) {
                        //calculate II
                        Integer[] IIList = calculateII(this.Nr, this.Ni,
                                new HashSet<>(messageValueNr.toList()),
                                new HashSet<>(messageValueNi.toList()));
                        for (Integer vertex : IIList) {
                            MapWritable outMsg = new MapWritable();

                            outMsg.put(incr, new MyVIntArrayWritable(IIList, vertex));
                            this.sendMessage(new VIntWritable(vertex), outMsg);
                        }
                    }
                    if (this.Nd.contains(senderId)) {
                        //calculate DD
                        Integer[] DDList = calculateDD(this.Nr, this.Nd,
                                new HashSet<>(messageValueNr.toList()),
                                new HashSet<>(messageValueNd.toList()));
                        for (Integer vertex : DDList) {
                            MapWritable outMsg = new MapWritable();

                            outMsg.put(decr, new MyVIntArrayWritable(DDList, vertex));
                            this.sendMessage(new VIntWritable(vertex), outMsg);
                        }
                    }
                }
                break;
            case 3:
                incr = new Text("PU+");
                decr = new Text("PU-");
                for (MapWritable message : messages) {
                    if (message.containsKey(incr)) {
                        List<Integer> s = ((MyVIntArrayWritable) message.get(incr)).toList();

                        updatePropinquity(s, UpdatePropinquity.INCREASE);
                    } else if (message.containsKey(decr)) {
                        List<Integer> s = ((MyVIntArrayWritable) message.get(decr)).toList();

                        updatePropinquity(s, UpdatePropinquity.DECREASE);
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
        for (Integer e : this.Nr) {
            this.addEdge(new Edge<>(new VIntWritable(e), new VIntWritable(this.P.get(e))));
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
        this.Nd.addAll(((MyVIntArrayWritable) this.getValue().get(k)).toList());

        k.set("Ni");
        this.Ni.addAll(((MyVIntArrayWritable) this.getValue().get(k)).toList());

        k.set("Nr");
        this.Nr.addAll(((MyVIntArrayWritable) this.getValue().get(k)).toList());

        k.set("P");
        MapWritable m = (MapWritable) this.getValue().get(k);
        for (Entry<Writable, Writable> e : m.entrySet()) {
            Integer key = ((IntWritable) e.getKey()).get();
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
        for (Entry<Integer, Integer> e : this.P.entrySet()) {
            Pmap.put(new IntWritable(e.getKey()), new IntWritable(e.getValue()));
        }
        valueMap.put(new Text("P"), Pmap);

        valueMap.put(new Text("Nr"), new MyVIntArrayWritable(this.Nr));
        valueMap.put(new Text("Ni"), new MyVIntArrayWritable(this.Ni));
        valueMap.put(new Text("Nd"), new MyVIntArrayWritable(this.Nd));

        valueMap.put(new Text("incrementalStep"), this.incrementalStep);
        valueMap.put(new Text("initializeStep"), this.initializeStep);
        valueMap.put(new Text("mainStep"), this.mainStep);

        this.setValue(valueMap);
    }
}

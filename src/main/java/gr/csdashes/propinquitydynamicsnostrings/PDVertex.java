package gr.csdashes.propinquitydynamicsnostrings;

import static gr.csdashes.propinquitydynamicsnostrings.CalculationTable.calculateDD;
import static gr.csdashes.propinquitydynamicsnostrings.CalculationTable.calculateII;
import static gr.csdashes.propinquitydynamicsnostrings.CalculationTable.calculateRD;
import static gr.csdashes.propinquitydynamicsnostrings.CalculationTable.calculateRI;
import static gr.csdashes.propinquitydynamicsnostrings.CalculationTable.calculateRR;
import gr.csdashes.propinquitydynamicsnostrings.io.MapMessage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class PDVertex extends Vertex<VIntWritable, VIntWritable, MapMessage> {

    Set<Integer> Nr = new HashSet<>(50); // The remaining neighboors
    Set<Integer> Ni = new HashSet<>(50); // The neighboors to be insterted
    Set<Integer> Nd = new HashSet<>(50); // The neighboors to be deleted
    // The propinquity value map
    Map<Integer, Integer> P = new HashMap<>(150);
    //cutting thresshold
    int a;
    //emerging value
    int b;

    private final Step mainStep = new Step(2);
    private final Step initializeStep = new Step(6);
    private final Step incrementalStep = new Step(4);

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

    // Message tags
    private final Integer initTag = 0;
    private final Integer intersectionTag = 1;
    private final Integer NrTag = 2;
    private final Integer puplusTag = 3;
    private final Integer puminusTag = 4;
    private final Integer senderTag = 5;
    private final Integer dnNrTag = 6;
    private final Integer dnNiTag = 7;
    private final Integer dnNdTag = 8;
    
    /* This method is responsible to initialize the propinquity
     * hash map in each vertex. Consists of 2 steps, the angle
     * and the conjugate propinquity update.
     * @param messages The messages received in each superstep.
     */
    private void initialize(Iterable<MapMessage> messages) throws IOException {        
        switch (this.initializeStep.getStep()) {
            /* Create an undirected graph. From each vertex send
             * our vertex id to all of the neighboors.
             */
            case 0:
                MapMessage outMsg = new MapMessage();                
                
                outMsg.put(this.initTag, this.getVertexID());
                this.sendMessageToNeighbors(outMsg);
                break;
            case 1:
                List<Edge<VIntWritable, VIntWritable>> edges = this.getEdges();
                Set<Integer> uniqueEdges = new HashSet<>(50);
                for (Edge<VIntWritable, VIntWritable> edge : edges) {
                    uniqueEdges.add(edge.getDestinationVertexID().get());
                }
                for (MapMessage message : messages) {
                    Integer id = ((Set<Integer>) message.get(this.initTag)).iterator().next();
                    if (uniqueEdges.add(id)) {
                        Edge<VIntWritable, VIntWritable> e = new Edge<>(new VIntWritable(id), new VIntWritable(0));
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
                outMsg = new MapMessage();
                for (Integer v : this.Nr) {
                    outMsg.put(this.NrTag, this.Nr, v);

                    this.sendMessage(new VIntWritable(v), outMsg);
                    outMsg = new MapMessage(); // TODO: Test if we can outMsg.clear()
                }
                break;
            case 3:
                /* Initialize the propinquity hash map for the vertexes of the
                 * received list.
                 */
                for (MapMessage message : messages) {
                    Set<Integer> commonNeighboors = (Set<Integer>) message.get(this.NrTag);
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
                outMsg = new MapMessage();
                Integer id = this.getVertexID().get();
                for (Integer neighboor : this.Nr) {
                    if (neighboor > id) {
                        // We dont need to clear, the key is overwritten
                        outMsg.put(this.NrTag, this.Nr, neighboor);
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
                Set<Integer> Nr_neighboors;
                for (MapMessage message : messages) {
                    Nr_neighboors = (Set<Integer>) message.get(this.NrTag);

                    boolean Nr1IsLarger = Nr.size() > Nr_neighboors.size();
                    Set<Integer> intersection = new HashSet<>(Nr1IsLarger ? Nr_neighboors : Nr);
                    intersection.retainAll(Nr1IsLarger ? Nr : Nr_neighboors);

                    for (Integer vertex : intersection) {
                        // If size == 1 this means that vertex is the intersection set
                        if (intersection.size() > 1) {
                            outMsg = new MapMessage(); // TODO: no need
                            outMsg.put(this.intersectionTag, intersection, vertex);
                            this.sendMessage(new VIntWritable(vertex), outMsg);
                        }
                    }
                }
                break;
            case 5:
                // update the conjugate propinquity
                for (MapMessage message : messages) {
                    Nr_neighboors = (Set<Integer>) message.get(this.intersectionTag);
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
    private void incremental(Iterable<MapMessage> messages) throws IOException {

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

                for (Integer vertex : this.Nr) {
                    VIntWritable target = new VIntWritable(vertex);
                    MapMessage outMsg = new MapMessage();

                    outMsg.put(this.puplusTag, this.Ni);
                    this.sendMessage(target, outMsg);

                    outMsg = new MapMessage();

                    outMsg.put(this.puminusTag, this.Nd);
                    this.sendMessage(target, outMsg);
                }
                for (Integer vertex : this.Ni) {
                    VIntWritable target = new VIntWritable(vertex);
                    MapMessage outMsg = new MapMessage();

                    outMsg.put(this.puplusTag, this.Nr);
                    this.sendMessage(target, outMsg);

                    outMsg = new MapMessage();

                    outMsg.put(this.puplusTag, this.Ni, vertex);
                    this.sendMessage(target, outMsg);
                }
                for (Integer vertex : this.Nd) {
                    VIntWritable target = new VIntWritable(vertex);
                    MapMessage outMsg = new MapMessage();

                    outMsg.put(this.puminusTag, this.Nr);
                    this.sendMessage(target, outMsg);

                    outMsg = new MapMessage();

                    outMsg.put(this.puminusTag, this.Nd, vertex);
                    this.sendMessage(target, outMsg);
                }
                break;
            case 1:
                for (MapMessage message : messages) {
                    if (message.containsKey(this.puplusTag)) {
                        Set<Integer> s = (Set<Integer>) message.get(this.puplusTag);
                        updatePropinquity(s, UpdatePropinquity.INCREASE);
                    } else if (message.containsKey(this.puminusTag)) {
                        Set<Integer> s = (Set<Integer>) message.get(this.puminusTag);
                        updatePropinquity(s, UpdatePropinquity.DECREASE);
                    }
                }

                for (Integer vertex : this.Nr) {
                    if ((vertex > this.getVertexID().get())) {
                        MapMessage outMsg = new MapMessage();

                        outMsg.put(this.senderTag, this.getVertexID());
                        outMsg.put(this.dnNrTag, this.Nr);
                        outMsg.put(this.dnNiTag, this.Ni);
                        outMsg.put(this.dnNdTag, this.Nd);
                        this.sendMessage(new VIntWritable(vertex), outMsg);
                    }
                }
                for (Integer vertex : this.Ni) {
                    if (vertex > this.getVertexID().get()) {
                        MapMessage outMsg = new MapMessage();

                        outMsg.put(this.senderTag, this.getVertexID());
                        outMsg.put(this.dnNrTag, this.Nr);
                        outMsg.put(this.dnNiTag, this.Ni);
                        this.sendMessage(new VIntWritable(vertex), outMsg);
                    }
                }
                for (Integer vertex : this.Nd) {
                    if (vertex > this.getVertexID().get()) {
                        MapMessage outMsg = new MapMessage();

                        outMsg.put(this.senderTag, this.getVertexID());
                        outMsg.put(this.dnNrTag, this.Nr);
                        outMsg.put(this.dnNdTag, this.Nd);
                        this.sendMessage(new VIntWritable(vertex), outMsg);
                    }
                }
                break;
            case 2:
                for (MapMessage message : messages) {
                    Integer senderId = ((Set<Integer>) message.get(this.senderTag)).iterator().next();

                    Set<Integer> messageValueNr = (Set<Integer>) message.get(this.dnNrTag);
                    Set<Integer> messageValueNi = (Set<Integer>) message.get(this.dnNiTag);
                    Set<Integer> messageValueNd = (Set<Integer>) message.get(this.dnNdTag);

                    if (messageValueNi == null) {
                        messageValueNi = new HashSet<>(10);
                    }
                    if (messageValueNd == null) {
                        messageValueNd = new HashSet<>(10);
                    }

                    if (this.Nr.contains(senderId)) {
                        //calculate RR
                        Set<Integer> RRList = calculateRR(this.Nr, messageValueNr);
                        //calculate RI
                        Set<Integer> RIList = calculateRI(this.Nr, this.Ni,
                                messageValueNr, messageValueNi);
                        //calculate RD
                        Set<Integer> RDList = calculateRD(this.Nr, this.Nd,
                                messageValueNr, messageValueNd);

                        for (Integer vertex : RRList) {
                            MapMessage outMsg = new MapMessage();

                            outMsg.put(this.puplusTag, RIList);
                            this.sendMessage(new VIntWritable(vertex), outMsg);

                            outMsg = new MapMessage();

                            outMsg.put(this.puminusTag, RDList);
                            this.sendMessage(new VIntWritable(vertex), outMsg);
                        }
                        for (Integer vertex : RIList) {
                            MapMessage outMsg = new MapMessage();

                            outMsg.put(this.puplusTag, RRList);
                            this.sendMessage(new VIntWritable(vertex), outMsg);

                            outMsg = new MapMessage();

                            outMsg.put(this.puplusTag, RIList, vertex);
                            this.sendMessage(new VIntWritable(vertex), outMsg);
                        }
                        for (Integer vertex : RDList) {
                            MapMessage outMsg = new MapMessage();

                            outMsg.put(this.puminusTag, RRList);
                            this.sendMessage(new VIntWritable(vertex), outMsg);

                            outMsg = new MapMessage();

                            outMsg.put(this.puminusTag, RDList, vertex);
                            this.sendMessage(new VIntWritable(vertex), outMsg);
                        }
                    }
                    if (this.Ni.contains(senderId)) {
                        //calculate II
                        Set<Integer> IIList = calculateII(this.Nr, this.Ni,
                                messageValueNr, messageValueNi);
                        for (Integer vertex : IIList) {
                            MapMessage outMsg = new MapMessage();

                            outMsg.put(this.puplusTag, IIList, vertex);
                            this.sendMessage(new VIntWritable(vertex), outMsg);
                        }
                    }
                    if (this.Nd.contains(senderId)) {
                        //calculate DD
                        Set<Integer> DDList = calculateDD(this.Nr, this.Nd,
                                messageValueNr, messageValueNd);
                        for (Integer vertex : DDList) {
                            MapMessage outMsg = new MapMessage();

                            outMsg.put(this.puminusTag, DDList, vertex);
                            this.sendMessage(new VIntWritable(vertex), outMsg);
                        }
                    }
                }
                break;
            case 3:
                for (MapMessage message : messages) {
                    if (message.containsKey(this.puplusTag)) {
                        Set<Integer> s = (Set<Integer>) message.get(this.puplusTag);
                        updatePropinquity(s, UpdatePropinquity.INCREASE);
                    } else if (message.containsKey(this.puminusTag)) {
                        Set<Integer> s = (Set<Integer>) message.get(this.puminusTag);
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
    public void compute(Iterable<MapMessage> messages) throws IOException {
        this.a = this.getConf().getInt("a", 0);
        this.b = this.getConf().getInt("b", 1000);

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
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        super.write(d);
        
        this.incrementalStep.write(d);
        this.initializeStep.write(d);
        this.mainStep.write(d);
        
        WritableUtils.writeVInt(d, this.Nr.size());
        for (Integer v : this.Nr) {
            WritableUtils.writeVInt(d, v);
        }
        
        WritableUtils.writeVInt(d, this.Ni.size());
        for (Integer v : this.Ni) {
            WritableUtils.writeVInt(d, v);
        }
        
        WritableUtils.writeVInt(d, this.Nd.size());
        for (Integer v : this.Nd) {
            WritableUtils.writeVInt(d, v);
        }
        
        WritableUtils.writeVInt(d, this.P.size());
        for (Entry<Integer, Integer> entry : this.P.entrySet()) {
            WritableUtils.writeVInt(d, entry.getKey());
            WritableUtils.writeVInt(d, entry.getValue());
        }
    }
    
    @Override
    public void readFields(DataInput di) throws IOException {
        super.readFields(di);
        
        this.incrementalStep.readFields(di);
        this.initializeStep.readFields(di);
        this.mainStep.readFields(di);
        
        int size = WritableUtils.readVInt(di);
        for (int i = 0; i < size; i++) {
            this.Nr.add(WritableUtils.readVInt(di));
        }
        
        size = WritableUtils.readVInt(di);
        for (int i = 0; i < size; i++) {
            this.Ni.add(WritableUtils.readVInt(di));
        }
        
        size = WritableUtils.readVInt(di);
        for (int i = 0; i < size; i++) {
            this.Nd.add(WritableUtils.readVInt(di));
        }
        
        // read P
        size = WritableUtils.readVInt(di);
        Integer k,v;
        for (int i = 0; i < size; i++) {
            k = WritableUtils.readVInt(di);
            v = WritableUtils.readVInt(di);
            this.P.put(k, v);
        }
    }
}

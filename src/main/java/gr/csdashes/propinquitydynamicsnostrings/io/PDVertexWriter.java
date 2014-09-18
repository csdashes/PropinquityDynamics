package gr.csdashes.propinquitydynamicsnostrings.io;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJobMessage;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexOutputWriter;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 * @param <KEYOUT>
 * @param <VALUEOUT>
 * @param <V>
 * @param <E>
 * @param <M>
 */
public class PDVertexWriter<KEYOUT extends Writable, VALUEOUT extends Writable, V extends WritableComparable, E extends Writable, M extends Writable> implements VertexOutputWriter<KEYOUT, VALUEOUT, V, E, M> {

    @Override
    public void setup(Configuration conf) {
        // do nothing
    }

    @Override
    public void write(Vertex<V, E, M> vertex, BSPPeer<Writable, Writable, KEYOUT, VALUEOUT, GraphJobMessage> peer) throws IOException {
        String s = "", prefix = "", delimeter = " ";
        for (Edge<V, E> e : vertex.getEdges()) {
            s += prefix + e.getDestinationVertexID() + "," + e.getValue().toString();
            prefix = delimeter;
        }
        peer.write((KEYOUT) vertex.getVertexID(), 
                (VALUEOUT) new Text(s));
    }
}

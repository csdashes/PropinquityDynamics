package gr.csdashes.propinquitydynamics.io;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class PDVertexReader extends VertexInputReader<LongWritable, Text, Text, NullWritable, LongWritable> {

    @Override
    public boolean parseVertex(LongWritable key, Text value, Vertex<Text, NullWritable, LongWritable> vertex) throws Exception {
        String[] k_v = value.toString().split("\t", 2);
        vertex.setVertexID(new Text(k_v[0]));

        String[] edges = k_v[1].split(" ");
        for (String e : edges) {
            vertex.addEdge(new Edge<Text, NullWritable>(new Text(e), null));
        }

        return true;
    }
}

package gr.csdashes.propinquitydynamics.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class PDVertexReader extends VertexInputReader<LongWritable, Text, Text, IntWritable, LongWritable> {

    @Override
    public boolean parseVertex(LongWritable key, Text value, Vertex<Text, IntWritable, LongWritable> vertex) throws Exception {
        String[] k_v = value.toString().split("\t", 2);
        vertex.setVertexID(new Text(k_v[0]));

        String[] edges = k_v[1].split(" ");
        for (String e : edges) {
            vertex.addEdge(new Edge<>(new Text(e), new IntWritable(0)));
        }

        return true;
    }
}

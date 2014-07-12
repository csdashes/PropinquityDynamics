package gr.csdashes.propinquitydynamics;

import gr.csdashes.propinquitydynamics.io.PDVertexReader;
import gr.csdashes.propinquitydynamics.io.PDVertexWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.GraphJob;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class Main {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            printUsage();
        }
        HamaConfiguration conf = new HamaConfiguration(new Configuration());
        GraphJob graphJob = createJob(args, conf);
        long startTime = System.currentTimeMillis();
        if (graphJob.waitForCompletion(true)) {
            System.out.println("Job Finished in "
                    + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        }
    }
    
    private static void printUsage() {
        System.out.println("Usage: <input> <output>");
        System.exit(-1);
    }

    private static GraphJob createJob(String[] args, HamaConfiguration conf) throws IOException {
//        conf.set("hama.graph.vertices.info", "org.apache.hama.graph.InMemoryVerticesInfo");
        
        GraphJob graphJob = new GraphJob(conf, Main.class);
        graphJob.setJobName("Propinquity Dynamics");
        graphJob.setVertexClass(PDVertex.class);
        graphJob.setJar("target/PropinquityDynamics-1.0-SNAPSHOT.jar");

        graphJob.setInputPath(new Path(args[0]));
        graphJob.setOutputPath(new Path(args[1]));

        graphJob.setVertexIDClass(Text.class);
        graphJob.setVertexValueClass(MapWritable.class);
        graphJob.setEdgeValueClass(IntWritable.class);

        graphJob.setInputFormat(TextInputFormat.class);
        graphJob.setInputKeyClass(LongWritable.class);
        graphJob.setInputValueClass(Text.class);

        graphJob.setVertexInputReaderClass(PDVertexReader.class);
        graphJob.setPartitioner(HashPartitioner.class);

        graphJob.setVertexOutputWriterClass(PDVertexWriter.class);
        graphJob.setOutputFormat(TextOutputFormat.class);
        graphJob.setOutputKeyClass(Text.class);
        graphJob.setOutputValueClass(Text.class);

        return graphJob;
    }
}

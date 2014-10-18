package gr.csdashes.propinquitydynamicsnostrings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class Step implements Writable {

    private int step;
    private int maxStep;
    private int iteration;

    Step() {
    }

    Step(int maxStep) {
        this.step = 0;
        this.maxStep = maxStep;
        this.iteration = 0;
    }

    /**
     * @return the step
     */
    public int getStep() {
        return step;
    }

    public void increaseStep() {
        this.step = (this.step + 1) % this.maxStep;
        if (this.step == 0) {
            this.iteration++;
        }
    }

    public int getIteration() {
        return this.iteration;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(this.step);
        d.writeInt(this.maxStep);
        d.writeInt(this.iteration);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        this.step = di.readInt();
        this.maxStep = di.readInt();
        this.iteration = di.readInt();
    }
}

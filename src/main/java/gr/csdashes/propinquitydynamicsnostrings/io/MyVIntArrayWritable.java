package gr.csdashes.propinquitydynamicsnostrings.io;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class MyVIntArrayWritable extends ArrayWritable {

    public MyVIntArrayWritable() {
        super(VIntWritable.class);
    }

    public MyVIntArrayWritable(VIntWritable[] values) {
        super(VIntWritable.class, values);
    }

    public MyVIntArrayWritable(Integer[] values) {
        super(VIntWritable.class);
        VIntWritable[] array = new VIntWritable[values.length];
        for (int i = 0; i < array.length; i++) {
            array[i] = new VIntWritable(values[i]);
        }

        this.set(array);
    }

    public MyVIntArrayWritable(Integer[] values, Integer skipString) {
        super(VIntWritable.class);
        VIntWritable[] array = new VIntWritable[values.length - 1];
        int i = 0;
        for (Integer v : values) {
            if (!v.equals(skipString)) {
                // The i++ first returns and then increases
                array[i++] = new VIntWritable(v);
            }
        }

        this.set(array);
    }

    public MyVIntArrayWritable(Collection<Integer> values) {
        super(VIntWritable.class);
        VIntWritable[] array = new VIntWritable[values.size()];
        int i = 0;
        for (Integer v : values) {
            array[i++] = new VIntWritable(v);
        }

        this.set(array);
    }

    public MyVIntArrayWritable(Collection<Integer> values, Integer skipString) {
        super(VIntWritable.class);
        VIntWritable[] array = new VIntWritable[values.size() - 1];
        int i = 0;
        for (Integer v : values) {
            if (!v.equals(skipString)) {
                array[i++] = new VIntWritable(v);
            }
        }

        this.set(array);
    }

    @Override
    public Integer[] toArray() {
        Writable[] arw = super.get();
        Integer[] ar = new Integer[super.get().length];
        for (int i = 0; i < super.get().length; i++) {
            ar[i] = ((VIntWritable) arw[i]).get();
        }
        return ar;
    }

    public List<Integer> toList() {
        return Arrays.asList(this.toArray());
    }

    public int size() {
        return super.get().length;
    }

    public boolean isEmpty() {
        return super.get().length == 0;
    }
}

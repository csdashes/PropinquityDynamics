package testing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class MapMessageInt extends HashMap<Integer, Set<Integer>> implements Writable {

    public MapMessageInt() {
    }

    public void put(Integer k, Set<Integer> v, Integer skipV) {
        Set<Integer> set = new HashSet<>(v);
        set.remove(skipV);

        this.put(k, set);
    }

    public void put(Integer k, Integer v) {
        Set<Integer> set = new HashSet<>(1);
        set.add(v);
        
        this.put(k, set);
    }

    public void put(Integer k, VIntWritable v) {
        this.put(k, v.get());
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(this.size());
        for (Entry<Integer, Set<Integer>> entry : this.entrySet()) {
            // Write each key
            d.writeInt(entry.getKey());
            // Write the size of the list
            d.writeInt(entry.getValue().size());
            // Write the list itself with VIntWritable
            for (Integer v : entry.getValue()) {
//                WritableUtils.writeVInt(d, v);
                d.writeInt(v);
            }
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        this.clear();

        int numOfKeys = di.readInt();
        for (int i = 0; i < numOfKeys; i++) {
            int key = di.readInt();
            // read the length of the set and create a HashSet
            int setSize = di.readInt();
            Set<Integer> set = new HashSet<>(setSize);
            // read the set
            for (int g = 0; g < setSize; g++) {
//                set.add(WritableUtils.readVInt(di));
                set.add(di.readInt());
            }
            this.put(key, set);
        }
    }
}

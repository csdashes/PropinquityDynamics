package gr.csdashes.propinquitydynamics.io;

import java.util.Collection;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class MyTextArrayWritable extends ArrayWritable {

    public MyTextArrayWritable() {
        super(Text.class);
    }

    public MyTextArrayWritable(Text[] values) {
        super(Text.class, values);
    }

    public MyTextArrayWritable(String[] values) {
        super(Text.class);
        Text[] textArray = new Text[values.length];
        for (int i = 0; i < textArray.length; i++) {
            textArray[i] = new Text(values[i]);
        }

        this.set(textArray);
    }

    public MyTextArrayWritable(String[] values, String skipString) {
        super(Text.class);
        Text[] textArray = new Text[values.length - 1];
        int i = 0;
        for (String v : values) {
            if (!v.equals(skipString)) {
                // The i++ first returns and then increases
                textArray[i++] = new Text(v);
            }
        }

        this.set(textArray);
    }
    
    public MyTextArrayWritable(Collection<String> values) {
        super(Text.class);
        Text[] textArray = new Text[values.size()];
        int i = 0;
        for (String v : values) {
            textArray[i++] = new Text(v);
        }

        this.set(textArray);
    }
    
    public MyTextArrayWritable(Collection<String> values, String skipString) {
        super(Text.class);
        Text[] textArray = new Text[values.size()-1];
        int i = 0;
        for (String v : values) {
            if (!v.equals(skipString)) {
                textArray[i++] = new Text(v);
            }
        }

        this.set(textArray);
    }

    public int size() {
        return super.get().length;
    }

    public boolean isEmpty() {
        return super.get().length == 0;
    }
}

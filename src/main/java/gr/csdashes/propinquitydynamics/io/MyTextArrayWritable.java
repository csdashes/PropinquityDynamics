package gr.csdashes.propinquitydynamics.io;

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
        for (int i = 0; i < textArray.length; i++) {
            if (!values[i].equals(skipString)) {
                textArray[i] = new Text(values[i]);
            }
        }

        this.set(textArray);
    }
}

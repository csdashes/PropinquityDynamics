package testing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class Main {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        int maxSet = 50;
        int maxKeys = 4;
        
        System.out.println("Test MapMessageInt");
        MapMessageInt mm = new MapMessageInt();
        
        for (int i = 0; i < maxKeys; i++) {
            Integer key = 123 + i;
            Set<Integer> set = new HashSet<>(maxSet);
            for (int g = 0; g < maxSet; g++) {
                set.add(g);
            }
            mm.put(key, set);
        }
        
        ByteArrayOutputStream mbos = new ByteArrayOutputStream();
        DataOutputStream mdos = new DataOutputStream(mbos);
        
        long start = System.nanoTime();
        mm.write(mdos);
        long elapsedTime = System.nanoTime() - start;
        
        System.out.println("Number of keys: " + maxKeys + " and size of sets: " + maxSet);
        System.out.println("Serialized length of MapMessage: " + mbos.toByteArray().length);
        System.out.println("nano-Time to serialize: " + elapsedTime);
        
        MapMessageInt mm2 = new MapMessageInt();
        
        ByteArrayInputStream asd = new ByteArrayInputStream(mbos.toByteArray());
        DataInputStream mios = new DataInputStream(asd);
        
        start = System.nanoTime();
        mm2.readFields(mios);
        elapsedTime = System.nanoTime() - start;
        System.out.println("nano-Time to deserialize: " + elapsedTime);

        System.out.println("Test MapMessageVint");
        MapMessageVint mm3 = new MapMessageVint();

        for (int i = 0; i < maxKeys; i++) {
            Integer key = 123 + i;
            Set<Integer> set = new HashSet<>(maxSet);
            for (int g = 0; g < maxSet; g++) {
                set.add(g);
            }
            mm3.put(key, set);
        }

        ByteArrayOutputStream mboss = new ByteArrayOutputStream();
        DataOutputStream mdoss = new DataOutputStream(mboss);

        start = System.nanoTime();
        mm3.write(mdoss);
        elapsedTime = System.nanoTime() - start;

        System.out.println("Number of keys: " + maxKeys + " and size of sets: " + maxSet);
        System.out.println("Serialized length of MapMessage: " + mboss.toByteArray().length);
        System.out.println("nano-Time to serialize: " + elapsedTime);

        MapMessageVint mm4 = new MapMessageVint();

        ByteArrayInputStream asds = new ByteArrayInputStream(mbos.toByteArray());
        DataInputStream mioss = new DataInputStream(asds);

        start = System.nanoTime();
        mm4.readFields(mioss);
        elapsedTime = System.nanoTime() - start;
        System.out.println("nano-Time to deserialize: " + elapsedTime);

        // VInt vs int!:
        /*
        INT!
        Number of keys: 4 and size of sets: 50
        Serialized length of MapMessage: 836
        mm2 size: 4
        nano-Time to serialize: 581.183
        nano-Time to deserialize: 218.499

        
        VINTWRITABLE:
        Number of keys: 4 and size of sets: 50
        Serialized length of MapMessage: 236
        mm2 size: 4
        
        and full VINT everywhere:
        Number of keys: 4 and size of sets: 50
        Serialized length of MapMessage: 209
        mm2 size: 4
        nano-Time to serialize: 1.564.159
        nano-Time to deserialize: 234.655
        */
    }
    
}

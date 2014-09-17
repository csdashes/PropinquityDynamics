package gr.csdashes.propinquitydynamics;

import com.google.common.collect.Sets;
import java.util.Set;

/**
 *
 * @author Anastasis Andronidis <anastasis90@yahoo.gr>
 */
public class CalculationTable {

    public static String[] calculateRR(Set<String> Nr, Set<String> nr) {

        if (Nr.size() > nr.size()) {
            return Sets.intersection(nr, Nr).toArray(new String[0]);
        }
        return Sets.intersection(Nr, nr).toArray(new String[0]);
    }

    public static String[] calculateII(Set<String> Nr, Set<String> Ni, Set<String> nr, Set<String> ni) {

        Set<String> t1;
        if (Nr.size() > Ni.size()) {
            t1 = Sets.union(Ni, Nr);
        } else {
            t1 = Sets.union(Nr, Ni);
        }

        Set<String> t2;
        if (ni.size() > nr.size()) {
            t2 = Sets.union(nr, ni);
        } else {
            t2 = Sets.union(ni, nr);
        }

        if (t1.size() > t2.size()) {
            return Sets.intersection(t2, t1).toArray(new String[0]);
        }

        return Sets.intersection(t1, t2).toArray(new String[0]);
    }

    public static String[] calculateDD(Set<String> Nr, Set<String> Nd, Set<String> nr, Set<String> nd) {

        Set<String> t1;
        if (Nr.size() > Nd.size()) {
            t1 = Sets.union(Nd, Nr);
        } else {
            t1 = Sets.union(Nr, Nd);
        }

        Set<String> t2;
        if (nd.size() > nr.size()) {
            t2 = Sets.union(nr, nd);
        } else {
            t2 = Sets.union(nd, nr);
        }

        if (t1.size() > t2.size()) {
            return Sets.intersection(t2, t1).toArray(new String[0]);
        }

        return Sets.intersection(t1, t2).toArray(new String[0]);
    }

    public static String[] calculateRI(Set<String> Nr, Set<String> Ni, Set<String> nr, Set<String> ni) {

        Set<String> t1;
        if (Nr.size() > ni.size()) {
            t1 = Sets.intersection(ni, Nr);
        } else {
            t1 = Sets.intersection(Nr, ni);
        }

        Set<String> t2;
        if (Ni.size() > nr.size()) {
            t2 = Sets.intersection(nr, Ni);
        } else {
            t2 = Sets.intersection(Ni, nr);
        }

        Set<String> t3;
        if (Ni.size() > ni.size()) {
            t3 = Sets.intersection(ni, Ni);
        } else {
            t3 = Sets.intersection(Ni, ni);
        }

        Set<String> u1;
        if (t1.size() > t2.size()) {
            u1 = Sets.union(t2, t1);
        } else {
            u1 = Sets.union(t1, t2);
        }

        if (u1.size() > t3.size()) {
            return Sets.union(t3, u1).toArray(new String[0]);
        }

        return Sets.union(u1, t3).toArray(new String[0]);
    }

    public static String[] calculateRD(Set<String> Nr, Set<String> Nd, Set<String> nr, Set<String> nd) {

        Set<String> t1;
        if (Nr.size() > nd.size()) {
            t1 = Sets.intersection(nd, Nr);
        } else {
            t1 = Sets.intersection(Nr, nd);
        }

        Set<String> t2;
        if (Nd.size() > nr.size()) {
            t2 = Sets.intersection(nr, Nd);
        } else {
            t2 = Sets.intersection(Nd, nr);
        }

        Set<String> t3;
        if (Nd.size() > nd.size()) {
            t3 = Sets.intersection(nd, Nd);
        } else {
            t3 = Sets.intersection(Nd, nd);
        }

        Set<String> u1;
        if (t1.size() > t2.size()) {
            u1 = Sets.union(t2, t1);
        } else {
            u1 = Sets.union(t1, t2);
        }

        if (u1.size() > t3.size()) {
            return Sets.union(t3, u1).toArray(new String[0]);
        }

        return Sets.union(u1, t3).toArray(new String[0]);
    }
}

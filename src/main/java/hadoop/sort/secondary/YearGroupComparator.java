package hadoop.sort.secondary;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class YearGroupComparator extends WritableComparator {

    public YearGroupComparator() {
        super(Combinekey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Combinekey k1 = (Combinekey) a;
        Combinekey k2 = (Combinekey) b;
        return k1.getYear()-k2.getYear();
    }
}

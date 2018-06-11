package sort.order;



import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupComparator extends WritableComparator {

    public OrderGroupComparator() {
        super(ComboKey.class,true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ComboKey c1 = (ComboKey) a;
        ComboKey c2 = (ComboKey) b;
        return c1.getCid() - c2.getCid();
    }
}

package hadoop.sort.secondary;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortTemp extends WritableComparator {
    public SortTemp() {
        super(Combinekey.class,true);
    }
    //自定义排序
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //按照年份升序排序 按照温度降序排序
        Combinekey o1=(Combinekey)a;
        Combinekey o2=(Combinekey)b;
        if(o1.getYear()!=o2.getYear()){
            return o1.getYear() - o2.getYear();
        }else{
            return  o2.getTemp() - o1.getTemp();
        }
    }
}

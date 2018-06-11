package sort.order;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ComboKey implements WritableComparable<ComboKey> {
    private int cid;
    private int flag;

    public ComboKey(){}

    public ComboKey(int cid, int flag) {
        this.cid = cid;
        this.flag = flag;
    }

    public int getCid() {
        return cid;
    }

    public void setCid(int cid) {
        this.cid = cid;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }


    public int compareTo(ComboKey o) {
        if(cid != o.cid){
            return cid - o.cid;
        }
        return flag - o.flag;
    }

    @Override
    public String toString() {
        return "(" +
                "cid=" + cid +
                ", flag=" + flag +
                ')';
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(cid);
        dataOutput.writeInt(flag);

    }

    public void readFields(DataInput dataInput) throws IOException {
        cid = dataInput.readInt();
        flag = dataInput.readInt();
    }
}

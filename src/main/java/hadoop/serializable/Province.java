package hadoop.serializable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Province implements Writable {

    private String name;
    private int count;

    public Province() {
    }

    public Province(String name, int count) {
        this.name = name;
        this.count = count;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeInt(count);
    }

    public void readFields(DataInput dataInput) throws IOException {
        name = dataInput.readUTF();
        count = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "Province{" +
                "name='" + name + '\'' +
                ", count=" + count +
                '}';
    }
}

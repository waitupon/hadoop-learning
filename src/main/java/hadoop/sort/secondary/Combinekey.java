package hadoop.sort.secondary;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Combinekey implements WritableComparable<Combinekey> {
    private int year;
    private int temp;

    public Combinekey() {

    }

    public Combinekey(int year, int temp) {
        this.year = year;
        this.temp = temp;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    public int compareTo(Combinekey o) {
        if(this.year != o.year){
            return this.year - o.year;
        }else{
            return o.temp - this.temp;
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(temp);

    }

    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        temp = dataInput.readInt();
    }

    @Override
    public String toString() {
        return year + " " + temp;
    }

    @Override
    public int hashCode() {
        return new Integer(year + temp).hashCode();
    }
}

package inputformat.db;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class WordWritable implements DBWritable,WritableComparable<WordWritable> {

    private String word;
    private int num;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public int compareTo(WordWritable o) {
        return new Text(this.word).compareTo(new Text(o.word));
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeInt(num);
    }

    public void readFields(DataInput dataInput) throws IOException {
        dataInput.readUTF();
        dataInput.readInt();
    }

    public void write(PreparedStatement pstt) throws SQLException {
        pstt.setString(1,word);
        pstt.setInt(2,num);
    }

    public void readFields(ResultSet resultSet) throws SQLException {

    }
}

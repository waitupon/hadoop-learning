package utils.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created by Administrator on 2018/6/11 0011.
 */
public class SerializeUtils {

    @Test
     public void test() throws Exception {
         Schema schema = new Schema.Parser().parse(new File("d://tmp/user.avsc"));
         GenericRecord user1 = new GenericData.Record(schema);
         user1.put("name", "Alyssa");
         user1.put("favorite_number", 256);

         GenericRecord user2 = new GenericData.Record(schema);
         user2.put("name", "Ben");
         user2.put("favorite_number", 7);
         user2.put("favorite_color", "red");



         File file = new File("d://tmp/users.avro");
         DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
         DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
         dataFileWriter.create(schema, file);
         dataFileWriter.append(user1);
         dataFileWriter.append(user2);
         dataFileWriter.close();
     }





}

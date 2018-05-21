package serializable;

import org.junit.Test;

import java.io.*;

public class TestJavaSerialize {



    @Test
    public void serialize() throws Exception {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);

        oos.writeObject(new Dog("dd3"));
        oos.close();
        baos.close();

        System.out.println(baos.toByteArray().length);

        ByteArrayInputStream bis = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bis);

        Dog dog = (Dog) ois.readObject();
        System.out.println(dog);

        ois.close();
        bis.close();
    }
}

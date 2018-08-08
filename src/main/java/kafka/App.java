package kafka;



import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.*;

/**
 * Created by Administrator on 2018/8/6 0006.
 */
public class App {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "master:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //生产者发送消息
        String topic = "mytopic";
        Producer<String, String> procuder = new KafkaProducer<String,String>(props);
        for (int i = 1; i <= 2; i++) {
            String value = "val_" + i;
            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, value);

            procuder.send(msg);
            System.out.println(value);
        }
        procuder.close();

        System.out.println("over");


    }


    @Test
    public void consumer(){
        Properties props = new Properties();
        props.put("zookeeper.connect", "master:2181");
        props.put("group.id", "test-1");
        props.put("auto.commit.interval.ms", "1000");



        ConsumerConfig conf = new ConsumerConfig(props);

        ConsumerConnector conn = Consumer.createJavaConsumerConnector(conf);

        Map<String,Integer> topicCount = new HashMap<String,Integer>();
        topicCount.put("mytopic",1);

        Map<String, List<KafkaStream<byte[], byte[]>>> map = conn.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> list = map.get("mytopic");
        for(KafkaStream<byte[], byte[]> data :list){
            ConsumerIterator<byte[], byte[]> iterator = data.iterator();
            while(iterator.hasNext()){
                MessageAndMetadata<byte[], byte[]> message = iterator.next();
                System.out.println( new String(message.message()));
            }

        }
        conn.shutdown();

    }
}

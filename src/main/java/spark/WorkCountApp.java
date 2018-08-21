package spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


import java.util.Arrays;
import java.util.Iterator;


/**
 * Created by Administrator on 2018/8/15 0015.
 */
public class WorkCountApp {

    public static void main(String[] args) {
        System.load("D:\\soft\\hadoop-2.7.6\\bin\\hadoop.dll");
        SparkConf conf = new SparkConf();
     //   conf.setMaster("spark://192.168.3.177:7077");
        conf.setMaster("local");
        conf.setAppName("WordCountApp");

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> rdd = context.textFile("file:///D:/a.txt",2);
        JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD< String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2< String, Integer> call(String s) throws Exception {
                return new Tuple2< String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) throws Exception {
                return x+y;
            }
        });

        counts.saveAsTextFile("file:///D:/cool");



    }
}

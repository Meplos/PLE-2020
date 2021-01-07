package bigdata;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import it.nerdammer.spark.hbase.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.api.java.JavaRDD;

public class RTStats {
    public static final String APP_NAME = "RT_STATS" ;
    public static final String MASTER = "yarn-client";
    public static final String HBASE_HOST = "http://10.0.203.3:16010";
    public static final String TABLE_NAME = "gresse_tweet_pop";

    public static void main(String[] args) {

        System.out.println("App Name : "+APP_NAME);
        System.out.println("Master : "+MASTER);
        System.out.println("HBase: "+HBASE_HOST);

        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER);
        JavaSparkContext sc = new JavaSparkContext(conf);
       
        JavaRDD<String> rawtweet = sc.textFile("/raw_data/tweet_01_03_2020");

        System.out.println(rawtweet.count());



        

    }
}
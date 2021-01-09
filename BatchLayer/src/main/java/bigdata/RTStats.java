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
import org.apache.spark.api.java.JavaPairRDD;

import com.google.gson.JsonParser;
import com.google.gson.JsonObject;

import scala.Tuple2;
public class RTStats {
    public static final String APP_NAME = "RT_STATS" ;
    public static final String HBASE_HOST = "http://10.0.203.3:16010";
    public static final String TABLE_NAME = "gresse_tweet_pop";

    public static void main(String[] args) {

        System.out.println("App Name : "+APP_NAME);
        System.out.println("HBase: "+HBASE_HOST);
        int k = 10;
        SparkConf conf = new SparkConf().setAppName(APP_NAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
       
        JavaRDD<String> rawtweet = sc.textFile("/raw_data/tweet_02_03_2020.nljson");
        JavaPairRDD<Integer,String> tweetByRT = rawtweet.mapToPair(
            (tweet) -> {
                
                JsonParser parser = new JsonParser();
                JsonObject tweetJSON = null;
                Integer rt = -1;
                
                try {
                    tweetJSON = parser.parse(tweet).getAsJsonObject();
                    rt = new Integer(tweetJSON.get("retweet_count").getAsInt());
                    
                } catch (Exception e) {
                    //System.out.println("Bad Tweet");
                }
                
                return new Tuple2<Integer,String>(rt,tweet);
            }); 
            //tweetByRT.sortByKey(false).take(1).forEach(x -> System.out.println(x._2));
            //tweetByRT.countByKey().forEach((x,y) -> System.out.println(x+" "+y));
            
            
            
        

    }
}
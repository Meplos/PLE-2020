package bigdata;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
/**
 * UserStat
 */
public class UserStat {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("User_Stats");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rawtweet = sc.textFile("/raw_data/tweet_01_03_2020");
        JavaRDD<List<Tuple2>> allUserHastag = rawtweet.map(
            tweet -> {
                
            }
        );

    }
}
package bigdata;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import it.nerdammer.spark.hbase.*;

public class RTStats {
    public static final String APP_NAME = "RT_STATS" ;
    public static final String MASTER = "yarn-cluster";
    public static final String HBASE_HOST = "http://10.0.203.3:16010";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER);
        conf.set("spark.hbase.host", HBASE_HOST);
        SparkContext sc = new SparkContext(conf);
        Configuration hbaseConf = HBaseConfiguration.create();

        

    }
}
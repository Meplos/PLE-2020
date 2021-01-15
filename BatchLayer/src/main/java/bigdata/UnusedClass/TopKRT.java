package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.gson.JsonParser;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class TopKRT extends Configured implements Tool{

    private static final String OUTPUT_TABLE = "gresse_tweet_topk";

    public static class TopKRTMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // On parse chaque ligne en objet JSON
            JsonParser parser = new JsonParser();

            JsonObject tweetJSON = null;

            try {
                tweetJSON = parser.parse(value.toString()).getAsJsonObject();
            } catch (Exception e) {
                return;
            }

            try{
                int fav = tweetJSON.get("quote_count").getAsInt();
                int rt = tweetJSON.get("retweet_count").getAsInt();
                int rep = tweetJSON.get("reply_count").getAsInt();
                int quote = tweetJSON.get("favorite_count").getAsInt();
                int total = fav+rt+rep+quote;
                String id = tweetJSON.get("id_str").getAsString();

                context.write(NullWritable.get(), new Text(id+","+rt));
                
            }catch (Exception e) {
                return;
            }
		}
    }
    
    public static class TopKRTReducer extends TableReducer<NullWritable,Text,NullWritable> {
        
		public void reduce(NullWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

            TreeMap<Integer,String> topk = new TreeMap<Integer,String>();

            for(Text tweet : values){

                String[] params = tweet.toString().split(",");

                topk.put(Integer.valueOf(params[1]), params[0]);

                if(topk.size() > 10) topk.remove(topk.firstEntry());

            }

            int i=10;
            for(Map.Entry<Integer,String> e : topk.entrySet()){
                Put put = new Put(Bytes.toBytes(Integer.toString(i)));
                i--;
                put.add(Bytes.toBytes("rt"),Bytes.toBytes("count") , Bytes.toBytes(Integer.toString(e.getKey())));
                context.write(NullWritable.get(), put);
            }
        }

    }
    
    public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
        conf.set(TableOutputFormat.OUTPUT_TABLE, "gresse_tweet_topk");
		
		Job job = Job.getInstance(conf, "Topk Tweets RT");
        job.setJarByClass(TopKRT.class);
        
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_01_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_02_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_03_03_2020.nljson"), TextInputFormat.class);
    
        job.setMapperClass(TopKRTMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        
        TableMapReduceUtil.initTableReducerJob("gresse_tweet_topk", TopKRTReducer.class, job);
        job.setReducerClass(TopKRTReducer.class);
        job.setNumReduceTasks(1);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new TopKRT(), args));
	}
    
}

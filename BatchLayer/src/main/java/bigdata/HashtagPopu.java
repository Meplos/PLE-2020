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
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.gson.JsonParser;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class HashtagPopu extends Configured implements Tool{

    private static final String OUTPUT_TABLE = "gresse_hashtag_pop";

	public static class HashtagPopuMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            JsonParser parser = new JsonParser();
            JsonObject tweetJSON = null;

            try {
                tweetJSON = parser.parse(value.toString()).getAsJsonObject();
                String texte = tweetJSON.get("text").getAsString();
            } catch (Exception e) {
                return;
            }

            JsonArray hashtagsJson = tweetJSON.get("entities").getAsJsonObject().get("hashtags").getAsJsonArray();

            String date = tweetJSON.get("created_at").getAsString();
            String champs_date[] = date.split(" ");

            for (int i = 0; i < hashtagsJson.size(); i++) {
                String hashtag = hashtagsJson.get(i).getAsJsonObject().get("text").getAsString().toLowerCase();
                context.write(new Text(champs_date[1]+" "+champs_date[2]+","+hashtag), new IntWritable(1));
            }
		}
    }
    
    public static class HashtagPopuCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
        
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            
            int count = 0;
			for (IntWritable index : values) {
                count=count+index.get();
            }

            context.write(new Text(key.toString()), new IntWritable(count));
        }
	}

	public static class HashtagPopuReducer extends TableReducer<Text,IntWritable,NullWritable> {
        
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            
            int count = 0;
			for (IntWritable index : values) {
                count=count+index.get();
            }

            String[] result = key.toString().split(",");
            Put put = new Put(Bytes.toBytes(result[1]));
            put.add(Bytes.toBytes("days"),Bytes.toBytes(result[0]) , Bytes.toBytes(Integer.toString(count)));
            context.write(NullWritable.get(), put);
        }
    
	}

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
        conf.set(TableOutputFormat.OUTPUT_TABLE, "gresse_hashtag_pop");
		
		Job job = Job.getInstance(conf, "EvolutionInMarchHBase");
        job.setJarByClass(HashtagPopu.class);
        
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_01_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_02_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_03_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_04_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_05_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_06_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_07_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_08_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_09_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_10_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_11_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_12_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_13_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_14_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_15_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_16_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_17_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_18_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_19_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_20_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_21_03_2020.nljson"), TextInputFormat.class);
		
		job.setMapperClass(HashtagPopuMapper.class);
		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(HashtagPopuCombiner.class);

        TableMapReduceUtil.initTableReducerJob("gresse_hashtag_pop", HashtagPopuReducer.class, job);
        job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new HashtagPopu(), args));
	}

}


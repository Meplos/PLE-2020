package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import com.google.gson.JsonParser;
import com.google.gson.JsonObject;

import bigdata.RTTopK.RTtweet;

public class RTTopK extends Configured implements Tool{

    private static final String OUTPUT_TABLE = "gresse_tweet_topk";
	
	public static class RTtweet implements Writable {
		
		public String id;
		public int rts;
		
		public RTtweet() {
			this.id = "";
			this.rts = 0;
		}
		
		public RTtweet(String id, int rts) {
			this.id = id;
			this.rts = rts;
		}
		
		public void readFields(DataInput in) throws IOException {
			this.id = in.readUTF();
			this.rts = in.readInt();
		}
		
		public void write(DataOutput out) throws IOException {
			out.writeUTF(id);
			out.writeInt(rts);
		}
		
	}

	public static class RTTopKMapper extends Mapper<LongWritable,Text,NullWritable, Text> {

		private TreeMap<Integer, String> topk = new TreeMap<Integer, String>();
		private int k = 10;

		@Override
		protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			this.k = context.getConfiguration().getInt("k", 10);
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// On parse chaque ligne en objet JSON
            JsonParser parser = new JsonParser();

            JsonObject tweetJSON = null;

            try {
				tweetJSON = parser.parse(value.toString()).getAsJsonObject();
				int rt = tweetJSON.get("retweet_count").getAsInt();
            } catch (Exception e) {
                return;
            }

			int rt = tweetJSON.get("retweet_count").getAsInt();

			String id = tweetJSON.get("id_str").getAsString();

			// on insere la ville dans le treemap
			topk.put(rt, id);
			
			// on retire jusqu'a n'en avoir plus que k
			// normalement effectue une seule fois
			while (topk.size() > k)
				topk.remove(topk.firstKey());
			
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
                    
            RTtweet val = new RTtweet();
			//ecrire les k villes restantes
			for(Map.Entry<Integer,String> pair : topk.entrySet()) {
				val.rts = pair.getKey();
				val.id = pair.getValue();
				context.write(NullWritable.get(), new Text(pair.getValue()+","+pair.getKey()));
			}
		}
	}
	
	public static class RTTopKCombiner extends Reducer<NullWritable, RTtweet, NullWritable, RTtweet> {
		private int k = 10;
		
		@Override
		protected void setup(Reducer<NullWritable, RTtweet, NullWritable, RTtweet>.Context context)
				throws IOException, InterruptedException {
			this.k = context.getConfiguration().getInt("k", 10);
		}

		@Override
		protected void reduce(NullWritable key, Iterable<RTtweet> values,
				Reducer<NullWritable, RTtweet, NullWritable, RTtweet>.Context context)
				throws IOException, InterruptedException {

			TreeMap<Integer, String> topk = new TreeMap<Integer, String>();
			for(RTtweet tweet : values) {
				//insertion de la ville
				topk.put(tweet.rts, tweet.id);
				// on conserve les k plus grandes
				while(topk.size() > k) {
					topk.remove(topk.firstKey());
				}
			}
			
			// ecrire les k plus grandes
			for (Map.Entry<Integer, String> v : topk.entrySet()) {
				context.write(NullWritable.get(), new RTtweet(v.getValue(), v.getKey()));
			}
		}
	}
	
	public static class RTTopKReducer extends TableReducer<NullWritable, Text, NullWritable> {
		
		private int k = 10;
		
		@Override
		protected void setup(TableReducer<NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			this.k = context.getConfiguration().getInt("k", 10);
		}

		@Override
		protected void reduce(NullWritable key, Iterable<Text> values,
				TableReducer<NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			
			TreeMap<Integer, String> topk = new TreeMap<Integer, String>();
			for(Text tweet : values) {
				//insertion de la ville
				String[] params = tweet.toString().split(",");
				topk.put(Integer.valueOf(params[1]), params[0]);
				// on conserve les k plus grandes
				while(topk.size() > k) {
					topk.remove(topk.firstKey());
				}
            }
            
            /*for(int i=1; i<k+1; i++){
                Put put = new Put(Bytes.toBytes(Integer.toString(i)));
                put.add(Bytes.toBytes("rt"),Bytes.toBytes("id") , Bytes.toBytes(Integer.toString(i)));
                context.write(NullWritable.get(), put);
            }*/
			
            // ecrire les k plus grandes
            int index = 1;
			for (Map.Entry<Integer, String> v : topk.entrySet()) {
				Put put = new Put(Bytes.toBytes(Integer.toString(index)));
				index++;
                put.add(Bytes.toBytes("rt"),Bytes.toBytes("id") , Bytes.toBytes(Integer.toString(v.getKey())));
			}
			
		}
		
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//int k = 0;
		//k = Integer.parseInt(args[0]);
        conf.setInt("k", 10);
        conf.set(TableOutputFormat.OUTPUT_TABLE, "gresse_tweet_topk");
		
		Job job = Job.getInstance(conf, "TweetsRTTopK");
        job.setJarByClass(RTTopK.class);
        
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_01_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_02_03_2020.nljson"), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_03_03_2020.nljson"), TextInputFormat.class);
        /*MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_04_03_2020.nljson"), TextInputFormat.class);
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
        MultipleInputs.addInputPath(job, new Path("/raw_data/tweet_21_03_2020.nljson"), TextInputFormat.class);*/
		
		job.setMapperClass(RTTopKMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        //job.setCombinerClass(RTTopKCombiner.class);
		
        TableMapReduceUtil.initTableReducerJob("gresse_tweet_topk", RTTopKReducer.class, job);
        
        job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new RTTopK(), args));
	}

}

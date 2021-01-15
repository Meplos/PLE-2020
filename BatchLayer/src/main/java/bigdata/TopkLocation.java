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
import com.google.gson.JsonObject;

public class TopkLocation extends Configured implements Tool{

    private static final String OUTPUT_TABLE = "gresse_location_topk";

	public static class TopkLocationMapper	extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private String lang = "";

        @Override
		public void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			this.lang = context.getConfiguration().get("lang");
		}
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // On parse chaque ligne en objet JSON
            JsonParser parser = new JsonParser();

            JsonObject tweetJSON = null;
            String tempLang = "";
            String location = "";

            try {
                tweetJSON = parser.parse(value.toString()).getAsJsonObject();
                tempLang = tweetJSON.get("lang").getAsString();
                location = tweetJSON.get("user").getAsJsonObject().get("location").getAsString().toLowerCase();
            } catch (Exception e) {
                return;
            }

            if(location.contains(",")){
                String[] loc = location.split(",");
                if(loc.length>0){
                    location = loc[0]; 
                }
            }
            

            if(tempLang.equals(this.lang)){

                // On renvoie le couple date / int
                context.write(new Text(location), new IntWritable(1));
            }
            
		}
    }
    
    public static class TopkLocationCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
        
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

            int total = 0;
            for(IntWritable i : values){

                total++;

            }

            context.write(new Text(key.toString()), new IntWritable(total));

        }

    }
    
    public static class TopkLocationReducer extends TableReducer<Text,IntWritable,NullWritable> {

        private int k = 0;
        private String lang = "";

        private TreeMap<Integer,String> topk = new TreeMap<Integer,String>();
        
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

            int total = 0;
            for(IntWritable i : values){

                total=total+i.get();

            }

            topk.put(total, key.toString());

        }

        @Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {

            this.k = context.getConfiguration().getInt("k", 10);
            this.lang = context.getConfiguration().get("lang","fr");
                    
            int index=topk.size();
            for(Map.Entry<Integer,String> e : topk.entrySet()){
                if(index <= this.k){
                    Put put = new Put(Bytes.toBytes(lang));
                    put.add(Bytes.toBytes("rank"),Bytes.toBytes(Integer.toString(index)) , Bytes.toBytes(e.getValue()));
                    context.write(NullWritable.get(), put);
                    Put put2 = new Put(Bytes.toBytes(lang));
                    put2.add(Bytes.toBytes("rank"),Bytes.toBytes(e.getValue()) , Bytes.toBytes(Integer.toString(e.getKey())));
                    context.write(NullWritable.get(), put2);
                }
                index--;
            }
		}

    }

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String lang = "fr";
		lang = args[0];
        conf.set("lang", lang);
        int k = 10;
		k = Integer.valueOf(args[1]);
        conf.setInt("k", k);
        conf.set(TableOutputFormat.OUTPUT_TABLE, "gresse_location_topk");
		
		Job job = Job.getInstance(conf, "Topk Location from lang");
        job.setJarByClass(TopkLocation.class);
        
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
		
		job.setMapperClass(TopkLocationMapper.class);
		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(TopkLocationCombiner.class);

        TableMapReduceUtil.initTableReducerJob("gresse_location_topk", TopkLocationReducer.class, job);
        job.setReducerClass(TopkLocationReducer.class);
        job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new TopkLocation(), args));
	}

}


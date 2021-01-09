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

public class EvolutionLang extends Configured implements Tool{

    private static final String OUTPUT_TABLE = "gresse_langage_evolution";

	public static class EvolutionLangMapper	extends Mapper<LongWritable, Text, Text, IntWritable> {
        
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

            try {
                tweetJSON = parser.parse(value.toString()).getAsJsonObject();
                tempLang = tweetJSON.get("lang").getAsString();
            } catch (Exception e) {
                return;
            }
            

            if(tempLang.equals(this.lang)){
                String date = tweetJSON.get("created_at").getAsString();
                String champs_date[] = date.split(" ");

                // On renvoie le couple date / int
                context.write(new Text(champs_date[1]+" "+champs_date[2]), new IntWritable(1));
            }
            
		}
    }
    
    public static class EvolutionLangCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            
            int count = 0;
			for (IntWritable index : values) {
                count=count+index.get();
            }
            
            context.write(new Text(key.toString()), new IntWritable(count));
        }

	}

	public static class EvolutionLangReducer extends TableReducer<Text,IntWritable,NullWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            
            int count = 0;
			for (IntWritable index : values) {
                count=count+index.get();
            }
            
            Put put = new Put(Bytes.toBytes(context.getConfiguration().get("lang")));
            put.add(Bytes.toBytes(key.toString()),Bytes.toBytes("count") , Bytes.toBytes(Integer.toString(count)));
            context.write(NullWritable.get(), put);
        }

	}

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String lang = "fr";
		lang = args[0];
        conf.set("lang", lang);
        conf.set(TableOutputFormat.OUTPUT_TABLE, "gresse_langage_evolution");
		
		Job job = Job.getInstance(conf, "EvolutionInMarchHBase");
        job.setJarByClass(EvolutionLang.class);
        
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
		
		job.setMapperClass(EvolutionLangMapper.class);
		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(EvolutionLangCombiner.class);

        TableMapReduceUtil.initTableReducerJob("gresse_langage_evolution", EvolutionLangReducer.class, job);
        job.setReducerClass(EvolutionLangReducer.class);
        job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new EvolutionLang(), args));
	}

}


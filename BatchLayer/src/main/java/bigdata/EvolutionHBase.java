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

public class EvolutionHBase extends Configured implements Tool{

    private static final String OUTPUT_TABLE = "gresse_word_pop";

	public static class EvolutionHBaseMapper	extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private String word = "";

        @Override
		public void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			this.word = context.getConfiguration().get("word");
		}
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // On parse chaque ligne en objet JSON
            JsonParser parser = new JsonParser();

            JsonObject tweetJSON = null;
            String texte = "";

            try {
                tweetJSON = parser.parse(value.toString()).getAsJsonObject();
                texte = tweetJSON.get("text").getAsString();
                texte.toLowerCase();
            } catch (Exception e) {
                return;
            }
            

            // On récupère le texte du tweet
            String champs[] = texte.split(" ");

            // Si le tweet est seulement un RT et pas une citation on ne le prend pas en compte
            if(champs[0]=="RT" && tweetJSON.get("is_quote_status")!=null) return ;

            // Si le texte ne contient pas le mot cherché on le prend pas en ciompte
            if(!texte.contains(word)) return ;

            String date = tweetJSON.get("created_at").getAsString();
            String champs_date[] = date.split(" ");

            // On renvoie le couple date / int
			context.write(new Text(champs_date[1]+" "+champs_date[2]), new IntWritable(1));
		}
    }
    
    public static class EvolutionHBaseCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
        
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            
            int count = 0;
			for (IntWritable index : values) {
                count++;
            }
            
            context.write(new Text(key.toString()), new IntWritable(count));
        }

	}

	public static class EvolutionHBaseReducer extends TableReducer<Text,IntWritable,NullWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            
            int count = 0;
			for (IntWritable val : values) {
                count=count+val.get();
            }

            Put put = new Put(Bytes.toBytes(context.getConfiguration().get("word")));
            put.add(Bytes.toBytes("number"),Bytes.toBytes(key.toString()) , Bytes.toBytes(Integer.toString(count)));
            context.write(NullWritable.get(), put);
        }

	}

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String word = "";
		word = args[0];
        conf.set("word", word);
        conf.set(TableOutputFormat.OUTPUT_TABLE, "gresse_word_pop");
		
		Job job = Job.getInstance(conf, "EvolutionInMarchHBase");
        job.setJarByClass(EvolutionHBase.class);
        
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
		
		job.setMapperClass(EvolutionHBaseMapper.class);
		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(EvolutionHBaseCombiner.class);

        TableMapReduceUtil.initTableReducerJob("gresse_word_pop", EvolutionHBaseReducer.class, job);
        job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new EvolutionHBase(), args));
	}

}


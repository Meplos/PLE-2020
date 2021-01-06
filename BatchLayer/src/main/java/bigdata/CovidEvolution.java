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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.gson.JsonParser;
import com.google.gson.JsonObject;

public class CovidEvolution extends Configured implements Tool{

	public static class CovidEvolutionMapper	extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private String word = "";

        @Override
		public void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			this.word = context.getConfiguration().get("word");
		}
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // On parse chaque ligne en objet JSON
            JsonParser parser = new JsonParser();

            JsonObject tweetJSON = parser.parse(value.toString()).getAsJsonObject();

            /*if(tweetJSON.isJsonNull()){
                System.out.println("tweetJSON is null");
                return;
            }*/

            String texte = "";

            try {
                texte = tweetJSON.get("text").getAsString();
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

	public static class CovidEvolutionReducer extends Reducer<Text,IntWritable,Text,Text> {

        private TreeMap<String, Integer> march = null;

        @Override
		public void setup(Reducer<Text, IntWritable, Text, Text>.Context context)
        throws IOException, InterruptedException {
			this.march = new TreeMap<String, Integer>();
        }
        
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            
            int count = 0;
			for (IntWritable index : values) {
                count++;
            }
            
            this.march.put(key.toString(), count);
        }
        
        @Override
        public void cleanup(Reducer<Text, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			for(Map.Entry<String,Integer> pair : march.entrySet()) {
				context.write(new Text(pair.getKey()), new Text(Integer.toString(pair.getValue())));
			}
		}
	}

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String word = "";
		word = args[0];
		conf.set("word", word);
		
		Job job = Job.getInstance(conf, "EvolutionInMarch");
		job.setNumReduceTasks(1);
		job.setJarByClass(CovidEvolution.class);
		
		job.setMapperClass(CovidEvolutionMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(CovidEvolutionReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new CovidEvolution(), args));
	}

}


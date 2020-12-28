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

import com.google.gson.JsonParser;
import com.google.gson.JsonObject;

import bigdata.TotalImpressionsByUser.Tweet;

public class TotalImpressionsByUser extends Configured implements Tool{

    public static class Tweet implements Writable {

        public String id;
        public int rt_total;
        public int fav_total;
        public int rep_total;

        public Tweet() {
            this.rt_total=0;
            this.fav_total=0;
            this.rep_total=0;
            this.id="";
        }

        public Tweet(String id, int rt_count, int fav_count, int rep_count) {
            this.rt_total=rt_count;
            this.fav_total=fav_count;
            this.rep_total=rep_count;
            this.id=id;
        }

        public void readFields(DataInput in) throws IOException {
			this.id = in.readUTF();
            this.rt_total = in.readInt();
            this.fav_total = in.readInt();
            this.rep_total = in.readInt();
		}
		
		public void write(DataOutput out) throws IOException {
			out.writeUTF(id);
            out.writeInt(rt_total);
            out.writeInt(fav_total);
            out.writeInt(rep_total);
		}

    }

	public static class TotalImpressionsByUserMapper	extends Mapper<LongWritable, Text, Text, Tweet> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // On parse chaque ligne en objet JSON
            JsonParser parser = new  JsonParser();
            JsonObject tweetJSON = parser.parse(value.toString()).getAsJsonObject();
            
            // On créé un user avec les données JSON
            Tweet tweet = new Tweet(tweetJSON.get("id_str"),tweetJSON.get("retweet_count"),tweetJSON.get("favorite_count"),tweetJSON.get("reply_count"));
			Text id = new Text();
            id.set(tweetJSON.get("id_str"));
            
            // On renvoie le couple id (Text) / user (User)
			context.write(id, tweet);
		}
	}

	public static class TotalImpressionsByUserReducer extends Reducer<Text,Tweet,Text,Text> {
		public void reduce(Text key, Iterable<Tweet> values,Context context) throws IOException, InterruptedException {
            int rt_total = 0;
            int fav_total = 0;
            int rep_total = 0;
			for (Tweet tweet : values) {
                rt_total = rt_total + tweet.rt_total;
                fav_total = fav_total + tweet.fav_total;
                rep_total = rep_total + tweet.rep_total;
			}
			context.write(key, new Text("Total RT : "+rt_total," Total Fav : "+fav_total," Total Reply : "+rep_total));
		}
	}

	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Clean population");
        job.setJarByClass(TotalImpressionsByUser.class);
        
        job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(User.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		try {
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
		} 
		catch (Exception e) {
			System.out.println(" bad arguments, waiting for 2 arguments [inputURI] [outputURI]");
			return -1;
		}
		job.setMapperClass(TotalImpressionsByUserMapper.class);
		job.setReducerClass(TotalImpressionsByUserReducer.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new FilterCities(), args));
	}

}

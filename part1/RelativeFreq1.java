import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RelativeFreq1 {
	public static class Map extends Mapper<Object, Text, WordPair, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private static final Text star = new Text("*");

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			Text current = null;
			if (itr.hasMoreTokens()) {
				current = new Text(itr.nextToken());
			}
			while (itr.hasMoreTokens()) {
				Text next = new Text(itr.nextToken());
				context.write(new WordPair(current, next), one);
				context.write(new WordPair(next, current), one);

				context.write(new WordPair(current, star), one);
				context.write(new WordPair(next, star), one);
				current = next;
			}
		}
	}

	public static class RFPartitioner extends Partitioner<WordPair, IntWritable> {
		@Override
		public int getPartition(WordPair key, IntWritable value, int numReduceTasks) {
			int hash = Math.abs(key.getWord().hashCode()) % numReduceTasks;
			return hash;
		}
	}

	private static class Combine extends Reducer<WordPair, IntWritable, WordPair, IntWritable> {
		public void reduce(WordPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for (Text value : values) {
				count += Integer.parseInt(value.toString());
			}
			context.write(key, new IntWritable(count));
		}
	}

	public static class Reduce extends Reducer<WordPair, IntWritable, WordPair, Text> {
		
		double totalCount = 0;
		public void reduce(WordPair key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;

			for (IntWritable value : values) {
				count += value.get();
			}

			if (key.getNeighbor().toString().contains("*")) {
				totalCount = count;
			} else {
				context.write(key, new Text(String.valueOf(count / totalCount)));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = new Job(new Configuration());
		job.setJarByClass(RelativeFreq1.class);
		job.setNumReduceTasks(3);

		job.setMapOutputKeyClass(WordPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(WordPair.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setPartitionerClass(RFPartitioner.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);

		FileInputFormat.addInputPath(job, new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("output"));

		job.waitForCompletion(true);
	}
}
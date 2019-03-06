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

	public static class RFPartitioner extends Partitioner<Text, IntWritable> {
		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			char[] alphabet = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
					't', 'u', 'v', 'w', 'x', 'y', 'z' };
			return (java.util.Arrays.binarySearch(alphabet, Character.toLowerCase(key.toString().split(",")[0].charAt(0)))
					* numReduceTasks / alphabet.length) % numReduceTasks;
		}
	}

	private static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for (Text value : values) {
				count += Integer.parseInt(value.toString());
			}
			context.write(key, new IntWritable(count));
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
		double totalCount = 0;

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			String keyStr = key.toString();
			int count = 0;

			for (IntWritable value : values) {
				count += value.get();
			}

			if (keyStr.contains("*")) {
				totalCount = count;
			} else {
				String[] pair = keyStr.split(",");
				context.write(new Text(pair[0] + ", " + pair[1]), new Text(String.valueOf(count / totalCount)));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = new Job(new Configuration());
		job.setJarByClass(RelativeFreq1.class);
		job.setNumReduceTasks(3);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
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
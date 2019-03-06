import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RelativeFreq1 {
	public static class Map extends Mapper<Object, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] words = value.toString().split(" ");

			for (String word : words) {
				if (word.matches("^\\w+$")) {
					int count = 0;
					for (String term : words) {
						if (term.matches("^\\w+$") && !term.equals(word)) {
							context.write(new Text(word + "," + term), one);
							count++;
						}
					}
					context.write(new Text(word + ",*"), new IntWritable(count));
				} else {
					System.out.println("word is: \"" + word + "\"");
				}
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

			if (keyStr.matches(".*\\*")) {
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
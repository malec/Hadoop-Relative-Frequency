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

public class relativefreq1 {
	public static class RFMapper extends Mapper<Object, Text, wordpair1, IntWritable> {
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
				context.write(new wordpair1(current, next), one);
				context.write(new wordpair1(next, current), one);

				context.write(new wordpair1(current, star), one);
				context.write(new wordpair1(next, star), one);
				current = next;
			}
		}
	}

	public static class RFPartitioner extends Partitioner<wordpair1, IntWritable> {
		@Override
		public int getPartition(wordpair1 key, IntWritable value, int numReduceTasks) {
			int hash = Math.abs(key.getWord().hashCode()) % numReduceTasks;
			return hash;
		}
	}

	private static class RFCombiner extends Reducer<wordpair1, IntWritable, wordpair1, IntWritable> {
		public void reduce(wordpair1 key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for (Text value : values) {
				count += Integer.parseInt(value.toString());
			}
			context.write(key, new IntWritable(count));
		}
	}

	public static class RFReducer extends Reducer<wordpair1, IntWritable, wordpair1, Text> {
		
		double totalCount = 0;
		public void reduce(wordpair1 key, Iterable<IntWritable> values, Context context)
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
		job.setJarByClass(relativefreq1.class);
		job.setNumReduceTasks(3);

		job.setMapOutputKeyClass(wordpair1.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(wordpair1.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(RFMapper.class);
		job.setPartitionerClass(RFPartitioner.class);
		job.setCombinerClass(RFCombiner.class);
		job.setReducerClass(RFReducer.class);

		FileInputFormat.addInputPath(job, new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("output"));

		job.waitForCompletion(true);
	}
}
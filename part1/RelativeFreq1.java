import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class RelativeFreq1 {
  public static class RF1Mapper extends Mapper<Object, Text, WordPair, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      Text current = null;
      Text star = new Text("*");
      if(itr.hasMoreTokens()) {
        current = new Text(itr.nextToken());
      }
      while (itr.hasMoreTokens()) {
        context.write(new WordPair(current, star), one);
        Text next = new Text(itr.nextToken());
        context.write(new WordPair(current, next), one);
        context.write(new WordPair(next, current), one);
        context.write(new WordPair(next, star), one);
        current = next;
      }
    }
  }

  public static class RF1Reducer extends Reducer<WordPair, IntWritable, WordPair, IntWritable> {
    public void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value : values) {
				sum = sum + value.get();
			}
			context.write(key, new IntWritable(sum));
    }
	}
	
	public static class RF1Partitioner extends Partitioner<WordPair, IntWritable> {
		@Override
		public int getPartition(WordPair key, IntWritable value, int numReduceTasks) {
			// System.out.println("key: " + key.getKey() + ", " + key.getValue() + " val:" +
			// value);
			return 
		}
	}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: WordSort <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(RelativeFreq1.class);
    job.setMapperClass(RF1Mapper.class);
    job.setCombinerClass(RF1Reducer.class);
		job.setReducerClass(RF1Reducer.class);
		job.setPartitionerClass(RF1Partitioner.class);
    job.setOutputKeyClass(WordPair.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
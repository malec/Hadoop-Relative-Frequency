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

public class RelativeFreq {
  public static class TokenizerMapper extends Mapper<Object, Text, WordPair, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      Text star = new Text("*");
      Text current = new Text(itr.nextToken());
      while (itr.hasMoreTokens()) {
        context.write(new WordPair(current, star), one);
        Text next = new Text(itr.nextToken());
        context.write(new WordPair(current, next), one);
        context.write(new WordPair(next, current), one);
        current = next;
      }
    }
  }

  public static class IntSumReducer extends Reducer<WordPair, IntWritable, WordPair, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(WordPair key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      IntWritable total = new IntWritable(0);
      Text start = new Text("*");
      for (IntWritable val : values) {
        if (key.getNeighbor() == start){
          // set the current total to this value
          System.out.println("the key is " + key.getWord() + "val" + key.getNeighbor());
          total = val;
        } else {
          context.write(key, new IntWritable(val.get()/total.get()));
        }
      }
      result.set(sum);
      context.write(key, result);
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
    job.setJarByClass(RelativeFreq.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(WordPair.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
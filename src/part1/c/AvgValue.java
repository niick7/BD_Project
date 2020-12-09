package part1.c;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgValue {
  public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().trim().split(" ");
      String ipAddr = line[0];
      int	   ipVal  = Integer.parseInt(line[line.length - 1]);
      context.write(new Text(ipAddr), new IntWritable(ipVal));
    }
  }

  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();
    int sum = 0;
    Float count = 0f;

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      for (IntWritable val : values) {
        sum += val.get();
        count += 1;
      }
      result.set(sum/count);
      context.write(new Text(key), result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "AvgValue");
    job.setJarByClass(AvgValue.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(Reduce.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

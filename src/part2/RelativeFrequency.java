package part2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RelativeFrequency {
  public static class TokenizerMapper
    extends Mapper<Object, Text, Pair, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      String[] strValues = value.toString().split(" ");
      Integer strLength = strValues.length;
      for(int i = 0; i < strLength; i++) {
        String valueI = strValues[i].trim();
        if(valueI.equals(""))
          continue;
        for(int j = 1; j < strLength; j++) {
          String valueJ = strValues[j].trim();
          if(valueJ.equals(""))
            continue;
          if(valueI.equals(valueJ))
            break;
          Pair pair = new Pair(valueI, valueJ);
          System.out.println(pair);
          context.write(pair, one);
        }
      }
    }
  }

  public static class IntSumReducer
    extends Reducer<Pair,IntWritable,Pair,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Pair pair, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(pair, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Relative Frequency");
    job.setJarByClass(RelativeFrequency.class);
    job.setMapperClass(RelativeFrequency.TokenizerMapper.class);
    job.setCombinerClass(RelativeFrequency.IntSumReducer.class);
    job.setReducerClass(RelativeFrequency.IntSumReducer.class);
    job.setOutputKeyClass(Pair.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

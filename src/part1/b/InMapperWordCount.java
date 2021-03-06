package part1.b;

import java.io.IOException;
import java.util.*;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InMapperWordCount {
  public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Map<String, Integer> map = new LinkedHashMap<>();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String word = itr.nextToken();
        if(map.containsKey(word)) {
          int cnt = (int)map.get(word) + 1;
          map.put(word, cnt);
        }
        else map.put(word, 1);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
      Iterator<Map.Entry<String, Integer>> temp = map.entrySet().iterator();

      while(temp.hasNext()) {
        Map.Entry<String, Integer> entry = temp.next();
        String keyVal 	= entry.getKey();
        Integer countVal= entry.getValue();

        context.write(new Text(keyVal), new IntWritable(countVal));
      }
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf, "InMapperWordCount");
    job.setJarByClass(InMapperWordCount.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }
}
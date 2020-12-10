package part4.a;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InMapperPairRelativeFrequency {
  public static class MyMapper extends Mapper<Object, Text, StrPairWritable, IntWritable> {
    Map <StrPairWritable, Integer> map = new LinkedHashMap<>();

    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
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
          StrPairWritable pair = new StrPairWritable(new Text(valueI), new Text(valueJ));
          if(map.containsKey(pair)) {
            int cnt = (int)map.get(pair) + 1;
            map.put(pair, cnt);
          }
          else {
            map.put(pair, 1);
          }
        }
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
      Iterator<Map.Entry<StrPairWritable, Integer>> temp = map.entrySet().iterator();

      while(temp.hasNext()) {
        Map.Entry<StrPairWritable, Integer> entry = temp.next();
        StrPairWritable keyVal 	= entry.getKey();
        Integer countVal= entry.getValue();
        context.write(new StrPairWritable(keyVal.getKey(), keyVal.getValue()), new IntWritable(countVal));
      }
    }
  }

  public static class MyReducer extends Reducer<StrPairWritable, IntWritable, StrPairWritable, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(StrPairWritable pair, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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

    Job job = Job.getInstance(conf, "InMapperPairRelativeFrequency");
    job.setJarByClass(InMapperPairRelativeFrequency.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    job.setOutputKeyClass(StrPairWritable.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

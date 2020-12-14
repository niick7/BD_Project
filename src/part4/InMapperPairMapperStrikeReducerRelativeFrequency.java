package part4;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InMapperPairMapperStrikeReducerRelativeFrequency {
  public static class MyMapper extends Mapper<Object, Text, StrPairWritable, DoubleWritable> {
    Map<StrPairWritable, Double> map = new LinkedHashMap<>();

    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] strValues = value.toString().split(" ");
      double strLength = strValues.length;
      for(int i = 0; i < strLength - 1; i++) {
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
            double cnt = (double)map.get(pair) + 1;
            map.put(pair, cnt);
          }
          else {
            map.put(pair, 1.0);
          }
        }
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
      Iterator<Map.Entry<StrPairWritable, Double>> temp = map.entrySet().iterator();

      while(temp.hasNext()) {
        Map.Entry<StrPairWritable, Double> entry = temp.next();
        StrPairWritable keyVal 	= entry.getKey();
        double countVal = entry.getValue();
        context.write(new StrPairWritable(keyVal.getKey(), keyVal.getValue()), new DoubleWritable(countVal));
      }
    }
  }

  public static class MyReducer extends Reducer<StrPairWritable, DoubleWritable, Text, Text> {
    private Text prevKey = new Text("");
    private Double total = 0.0;
    private MapWritable mapWritableValue = new MapWritable();

    public void reduce(StrPairWritable pair, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      double sum = 0.0;
      System.out.println(pair.toString());
      for (DoubleWritable value : values) {
        sum += value.get();
      }
      Text key = new Text(pair.getKey());
      if (!prevKey.equals(key)) {
        mapWritableValue.clear();
        prevKey.set(key.toString());
        total = 0.0;
      } else {
        total += sum;
        mapWritableValue.put(new Text(pair.getValue()), new DoubleWritable(sum/total));
      }
      context.write(new Text(key), new Text(mapWritableValue.entrySet().toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "InMapperPairMapperStrikeReducerRelativeFrequency");
    job.setJarByClass(InMapperPairMapperStrikeReducerRelativeFrequency.class);

    job.setMapOutputKeyClass(StrPairWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    job.setOutputKeyClass(StrPairWritable.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

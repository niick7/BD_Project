package part3;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripeRelativeFrequency {
  public static class MyMapper extends Mapper<Object, Text, Text, MapWritable> {
    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] strValues = value.toString().split(" ");
      int strLength = strValues.length;
      for(int i = 0; i < strLength - 1; i++) {
        String valueI = strValues[i].trim();
        MapWritable mapWritable = new MapWritable();
        if(valueI.equals(""))
          continue;
        for(int j = 1; j < strLength; j++) {
          String valueJ = strValues[j].trim();
          if(valueJ.equals(""))
            continue;
          if(valueI.equals(valueJ))
            break;
          Text valueJT = new Text(valueJ);
          if (mapWritable.containsKey(valueJT)) {
            DoubleWritable count = (DoubleWritable) mapWritable.get(valueJT);
            count.set(count.get() + 1);
          } else
            mapWritable.put(valueJT, new DoubleWritable(1));
        }
        context.write(new Text(valueI), mapWritable);
      }
    }
  }

  public static class MyReducer extends Reducer<Text, MapWritable, Text, Text> {
    private final MapWritable mapWritableValue = new MapWritable();

    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
      mapWritableValue.clear();
      for (MapWritable value : values) {
        addAll(value);
      }
      System.out.println(mapWritableValue.entrySet().toString());
      double total = 0.0;
      for (Writable k : mapWritableValue.keySet()) {
        DoubleWritable v = (DoubleWritable) mapWritableValue.get(k);
        total += v.get();
      }
      for (Writable k : mapWritableValue.keySet()) {
        DoubleWritable v = (DoubleWritable) mapWritableValue.get(k);
        v.set(v.get()/total);
      }
      context.write(key, new Text(mapWritableValue.entrySet().toString()));
    }

    public void addAll(MapWritable mapWritable) {
      Set<Writable> keys = mapWritable.keySet();
      for (Writable key : keys) {
        DoubleWritable mVValue = (DoubleWritable) mapWritable.get(key);
        if (mapWritableValue.containsKey(key)) {
          DoubleWritable outputMapWritableVal = (DoubleWritable) mapWritableValue.get(key);
          outputMapWritableVal.set(outputMapWritableVal.get() + mVValue.get());
        } else {
          mapWritableValue.put(key, mVValue);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "StripeRelativeFrequency");
    job.setJarByClass(StripeRelativeFrequency.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
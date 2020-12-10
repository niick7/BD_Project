package part3;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripeRelativeFrequency {
  public static class MyMapper extends Mapper<Object, Text, Text, MapWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] strValues = value.toString().split(" ");
      Integer strLength = strValues.length;
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
          mapWritable.put(new Text(valueJ), one);
        }
        context.write(new Text(valueI), mapWritable);
      }
    }
  }

  public static class MyReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
    private MapWritable mapWritableValue = new MapWritable();

    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
      mapWritableValue.clear();
      for (MapWritable value : values) {
        addAll(value);
      }
      context.write(key, mapWritableValue);
    }

    public void addAll(MapWritable mapWritable) {
      Set<Writable> keys = mapWritable.keySet();
      for (Writable key : keys) {
        IntWritable mVValue = (IntWritable) mapWritable.get(key);
        if (mapWritableValue.containsKey(key)) {
          IntWritable outputMapWritableVal = (IntWritable) mapWritableValue.get(key);
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

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MapWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
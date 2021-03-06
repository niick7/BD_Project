package part1.d;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InMapperAvgValue {
  public static class MyMapper extends Mapper<Object, Text, Text, IntPairWritable>{
    HashMap<String, Integer> mapSum = new HashMap<String, Integer>();
    HashMap<String, Integer> mapCount = new HashMap<String, Integer>();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().trim().split(" ");
      String ipAddr = line[0].trim();
      String strIpVal = line[line.length - 1].trim();
      if (!strIpVal.equals("-")) {
        int ipVal = Integer.parseInt(strIpVal);
        if (mapSum.containsKey(ipAddr)) {
          Integer sum = (Integer) mapSum.get(ipAddr) + ipVal;
          mapSum.put(ipAddr, sum);
        } else mapSum.put(ipAddr, ipVal);

        if (mapCount.containsKey(ipAddr)) {
          Integer count = (Integer) mapCount.get(ipAddr) + 1;
          mapCount.put(ipAddr, count);
        } else mapCount.put(ipAddr, 1);
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      Iterator<Map.Entry<String, Integer>> itr1 = mapSum.entrySet().iterator();

      while (itr1.hasNext()) {
        Entry<String, Integer> entry1 = itr1.next();
        String ipAddr = entry1.getKey();
        Integer sum = entry1.getValue();
        Integer count = (Integer) mapCount.get(ipAddr);
        // Emit
        context.write(new Text(ipAddr), new IntPairWritable(sum, count));
      }
    }
  }

  public static class MyReducer extends Reducer<Text, IntPairWritable, Text, FloatWritable> {
    protected void reduce(Text key, Iterable<IntPairWritable> values, Reducer<Text, IntPairWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
      String ipAddr = key.toString();
      int   sum = 0;
      Float cnt = 0f;
      for (IntPairWritable value:values) {
        sum = sum + value.getFirst();
        cnt = cnt + value.getSecond();
      }
      Float average = sum/cnt;
      context.write(new Text(ipAddr), new FloatWritable(average));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf, "InMapperAvgValue");
    job.setJarByClass(InMapperAvgValue.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntPairWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }
}

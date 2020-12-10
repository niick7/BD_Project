package part4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InMapperPairMapperStrikeReducerRelativeFrequency {
  public static class MyMapper extends Mapper<Object, Text, StrPairWritable, IntWritable> {
    Map<StrPairWritable, Integer> map = new LinkedHashMap<>();

    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] strValues = value.toString().split(" ");
      Integer strLength = strValues.length;
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


  public static class MyReducer extends Reducer<StrPairWritable, IntWritable, Text, MapWritable> {
    private Text prevKey = new Text("");
    private Double total = 0.0;
    private MapWritable mapWritableValue = new MapWritable();

    public void reduce(StrPairWritable pair, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      double sum = 0.0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      Text key = new Text(pair.getKey());
      System.out.println(pair.toString());
      if (!prevKey.equals(key)) {
        mapWritableValue.clear();
        prevKey = key;
        total = 0.0;
        context.write(new Text(key), mapWritableValue);
      }
      total += sum;
      mapWritableValue.put(new Text(pair.getValue()), new FloatWritable((float) (sum/total)));
      System.out.println(mapWritableValue.entrySet().toString());
	}
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "InMapperPairMapperStrikeReducerRelativeFrequency");
    job.setJarByClass(InMapperPairMapperStrikeReducerRelativeFrequency.class);

    job.setMapperClass(MyMapper.class);
    // job.setReducerClass(MyReducer.class);
    job.setReducerClass(HLMyReducer.class);

    job.setOutputKeyClass(StrPairWritable.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  
  public static class HLMyReducer extends Reducer<StrPairWritable, IntWritable, Text, DoubleWritable> {
//	  Method initialize:
//  ����	wPrev = null
//  ������H = new AssociativeArray();
//  ��� Method reduce (Pair(w, u), [C1, C2, C3, ...])
//  � � 	if (w != wPrev� && wPrev != null) then:
//  ������	total = total(H)
//  ����������Emit(wPrev, H / total)��������������������
//  ����������H.clear()
//  ������H{u} = sum(C1,C2�.)
//  ������wPrev = w
//  ����Method close:
//  ����	total = total(H)
//  ������Emit(wPrev, H / total)

	  // Method initialize:
	  private Text kPrev = new Text("");
	  private List<Double> mapH = new ArrayList<Double>();
	  private double totalH = 0.0;
	  double lastsum = 0.0;
	  // Method reduce (Pair(w, u), [C1, C2, C3, ...])
	  public void reduce(StrPairWritable pair, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		  // get sum = sum (C1, C2,...)
		  double sum = 0.0;
	      for (IntWritable value : values) {
	    	  sum += value.get();
	      }
	      lastsum = sum;
	      // check: if change key
		  Text key = new Text(pair.getKey());
		  if (!key.equals(kPrev) && !kPrev.equals("")) {
			  // Emit for previous Key
			  double total = getTotal(mapH);
			  context.write(new Text(kPrev), new DoubleWritable(sum/total));
			  // Reset and prepare for current Key
			  mapH = new ArrayList<Double>();
		  }
		  // ------ key continue
	      mapH.add(sum);
		  kPrev = key;
		  // update totalH
	      totalH += sum;
	  } // end reduce

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		// Emit for previous Key
		double total = getTotal(mapH);
		context.write(new Text(kPrev), new DoubleWritable(lastsum/total));	
	} 
	// Using for get total
	public double getTotal(List<Double> list) {
		double result = 0.0;
		for (int i = 0; i < list.size(); i++)
			result += list.get(i);
		// return value
		return result;
	} 
  }
}

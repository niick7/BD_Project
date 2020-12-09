package part3;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripeRelativeFrequency {
	// class Mapper
	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
	    // initialize
        private Map<String, Pair> map = new LinkedHashMap<>();

		private Map<String, Integer> map = new LinkedHashMap<>();
		
	    // map
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	        String w = "key";
	        // loop all in Window(w)
	        {
	        	
	        }
	    	
	    	
	    	
	    	StringTokenizer itr = new StringTokenizer(value.toString());
	        // loop to check each word
	        while (itr.hasMoreTokens()) {
	            String word = itr.nextToken();
	            // existed
	            if(map.containsKey(word)) {
	                int cnt = (int)map.get(word) + 1;
	                map.put(word, cnt);
	            } // not existed
	            else {
	                map.put(word, 1);
	            }
	        }
	    }
	    // close: method cleanup: create Mapper output
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
	}// end Mapper
}

//public class RelativeFrequency {
//	  public static class TokenizerMapper
//	    extends Mapper<Object, Text, Pair, IntWritable> {
//
//	    private final static IntWritable one = new IntWritable(1);
//
//	    public void map(Object key, Text value, Context context
//	    ) throws IOException, InterruptedException {
//	      String[] strValues = value.toString().split(" ");
//	      Integer strLength = strValues.length;
//	      for(int i = 0; i < strLength; i++) {
//	        String valueI = strValues[i].trim();
//	        if(valueI.equals(""))
//	          continue;
//	        for(int j = 1; j < strLength; j++) {
//	          String valueJ = strValues[j].trim();
//	          if(valueJ.equals(""))
//	            continue;
//	          if(valueI.equals(valueJ))
//	            break;
//	          Pair pair = new Pair(new Text(valueI), new Text(valueJ));
//	          context.write(pair, one);
//	        }
//	      }
//	    }
//	  }
//
//	  public static class IntSumReducer
//	    extends Reducer<Pair,IntWritable,Pair,IntWritable> {
//	    private IntWritable result = new IntWritable();
//
//	    public void reduce(Pair pair, Iterable<IntWritable> values,
//	                       Context context
//	    ) throws IOException, InterruptedException {
//	      int sum = 0;
//	      for (IntWritable val : values) {
//	        sum += val.get();
//	      }
//	      result.set(sum);
//	      context.write(pair, result);
//	    }
//	  }
//
//	  public static void main(String[] args) throws Exception {
//	    Configuration conf = new Configuration();
//	    Job job = Job.getInstance(conf, "RelativeFrequency");
//	    job.setJarByClass(RelativeFrequency.class);
//	    job.setMapperClass(RelativeFrequency.TokenizerMapper.class);
//	    job.setReducerClass(RelativeFrequency.IntSumReducer.class);
//	    job.setOutputKeyClass(Pair.class);
//	    job.setOutputValueClass(IntWritable.class);
//	    FileInputFormat.addInputPath(job, new Path(args[0]));
//	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
//	    System.exit(job.waitForCompletion(true) ? 0 : 1);
//	  }
//	}

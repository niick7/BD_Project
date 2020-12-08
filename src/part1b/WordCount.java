package org.myorg;

import java.io.IOException;
import java.util.*;
     
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
     
public class WordCount {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    // Using for in-Mapper
	    List <Pair> pairs = new ArrayList<Pair>();

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    String line = value.toString();
		    StringTokenizer tokenizer = new StringTokenizer(line);
		    while (tokenizer.hasMoreTokens()) {
		    	String word = tokenizer.nextToken();
		    	// check existed
				Pair pair = findPair (pairs, word);
				if (pair == null)
				{
					pairs.add(new Pair(word, 1));
					continue;
				}
				// Update existed
				pair.setValue(pair.getValue() + 1);
			}
		    // Write to Mapper Output
			for (int i = 0; i < pairs.size(); i++) {
				Pair pair = pairs.get(i);
				context.write(new Text(pair.getKey()), new IntWritable(pair.getValue()));
			}
		}
		// Using for in-Mapper
		public Pair findPair(List <Pair> list, String value) {
			for (int i = 0; i < list.size(); i++) {
				if (list.get(i).equals(value))
					return list.get(i);
			}
			// not Existed
		    return null;
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
		 
		Job job = new Job(conf, "wordcount");
		job.setJarByClass(WordCount.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		 
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		 
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		job.waitForCompletion(true);
	}
}
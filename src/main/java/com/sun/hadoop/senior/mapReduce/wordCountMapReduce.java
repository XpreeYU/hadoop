package com.sun.hadoop.senior.mapReduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MapReduce
 * @author root
 *
 */
public class wordCountMapReduce extends Configured implements Tool{

    //step 1: Map class
        /**
    * public class Mapper<Object, Object, Object, Object>
         *
         */
    public static class WordCountMapper extends 
    					Mapper<LongWritable, Text, Text, IntWritable>{

		private Text mapOutputKey = new Text();
		private final static IntWritable mapOutputValue = new IntWritable(1);
		
	    @Override
	    public void map(LongWritable key, Text value, Context context)
		    throws IOException, InterruptedException {
			// line value
			String lineValue = value.toString();
			
			// split
			//String[] strings = lineValue.split(" ");
			StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
			
			//iterator
			while (stringTokenizer.hasMoreElements()) {
			    //get word value
			    String wordValue = stringTokenizer.nextToken();
			    //	set value
			    mapOutputKey.set(wordValue);
			    	
			    //output
			    context.write(mapOutputKey, mapOutputValue);
			}
		
	    	}
    	}
    
    //step 2: Reduce class
    public static class WordCountReducer extends 
    					Reducer<Text, IntWritable, Text, IntWritable>{

    	private final static IntWritable outputValue = new IntWritable(1);
	
    	@Override
    	public void reduce(Text key, Iterable<IntWritable> values, Context context)
    		throws IOException, InterruptedException {
	    		// sum tmp
	    		int sum = 0;
	    		// iterator
	    		for(IntWritable value : values){
    	    		 //total
	    		     sum += value.get();
    	    			}
	    		// set value
	    		outputValue.set(sum);
	    		// output
	    		context.write(key, outputValue);
    		}
    	    
    	}
    
    
    //step 3 : Driver, component job
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    		// 1. get configuration
        	Configuration configuration = getConf();
	
			//2. create job
			Job job = Job.getInstance(configuration, this.getClass().getName());
			// run job
			job.setJarByClass(this.getClass());
	
			//4.set job
			// input -> map --> reduce -> output
			//3.1 input
			Path inPath = new Path(args[0]);
			FileInputFormat.addInputPath(job, inPath);
			
			//3.2 map
			job.setMapperClass(WordCountMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
	
			//3.3 reduce
			job.setReducerClass(WordCountReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			//3.4 output
			Path outPath = new Path(args[1]);
			FileOutputFormat.setOutputPath(job, outPath);
			
			//4. commit show logs
			boolean isSucess = job.waitForCompletion(true);
			
			return isSucess ? 0 : 1;
    	}
    
    
    //step 4. run program
    public static void main(String[] args) throws Exception {
	
    	// 1. get configuration
    	Configuration configuration = new Configuration();
		
    	//	int status = new wordCountMapReduce().run(args);
    		
    	int status = ToolRunner.run(configuration, new wordCountMapReduce(), args);
		
		System.exit(status);
    	}

}

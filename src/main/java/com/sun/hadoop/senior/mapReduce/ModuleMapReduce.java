package com.sun.hadoop.senior.mapReduce;

import java.io.IOException;

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
public class ModuleMapReduce extends Configured implements Tool{

    //step 1: Map class
        /**
    * public class Mapper<Object, Object, Object, Object>
         *
         */
    //TODO
    public static class ModuleMapper extends 
    					Mapper<LongWritable, Text, Text, IntWritable>{
		
		
	
	    @Override
	    public void cleanup(Context context) throws IOException,
		    InterruptedException {
			//Nothing
	    	}

	    @Override
	    public void map(LongWritable key, Text value, Context context)
		    throws IOException, InterruptedException {
			// TODO
			
	    	}
	    
	    @Override
	    public void setup(Context context) throws IOException,
		    InterruptedException {
			//Nothing
	    	}
    	}
    
    //step 2: Reduce class
    // TODO
    public static class ModuleReducer extends 
    					Reducer<Text, IntWritable, Text, IntWritable>{


    	@Override
    	public void setup(Context context)
    		throws IOException, InterruptedException {
    	    //Nothing
    		}
    
		@Override
    	public void reduce(Text key, Iterable<IntWritable> values, Context context)
    		throws IOException, InterruptedException {
  	    	// TODO
    	    		
    		}
    	    
		@Override
	    public void cleanup(Context context)
			throws IOException, InterruptedException {
		  //Nothing
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
	
			//3.set job
			// input -> map --> reduce -> output
			//3.1 input
			Path inPath = new Path(args[0]);
			FileInputFormat.addInputPath(job, inPath);
			
			//3.2 map
			job.setMapperClass(ModuleMapper.class);
			// TODO
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
	
//**********************Shuffle*******************************************

			//1. partitioner
			//job.setPartitionerClass(cls);
			
			//2. sort
//			job.setSortComparatorClass(cls);
	
			//3. optional, combiner
//			job.setCombinerClass(cls);
			
			//4. group
//			job.setGroupingComparatorClass(cls);
			
			
//************************Shuffle*****************************************
			
			//3.3 reduce
			job.setReducerClass(ModuleReducer.class);
			// TODO
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			// set reduce number task
//			job.setNumReduceTasks(1);
			
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
    	
		//set compress
//		configuration.set("mapreduce.map.output.compress", "true");
//		configuration.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
//    	configuration.set("mapreduce.job.reduces", "2");
    	
    	int status = ToolRunner.run(configuration, new ModuleMapReduce(), args);
		
		System.exit(status);
    	}

}

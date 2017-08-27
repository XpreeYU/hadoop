package com.sun.hadoop.senior.hdfs;

import java.io.File;
import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 
 * @author root
 * 
 */
public class HdfsApp {
    	
    //get filesSystem
	public static FileSystem getFileSystem() throws Exception {
    	// core-site.xml. core-defautl.xml hdfs-site.xml
    	Configuration conf = new Configuration();
    	
    	//get filesSystem
    	FileSystem fileSystem = FileSystem.get(conf);
    	
    	System.out.println(fileSystem);
	
		return fileSystem;
	}
    
	/**
	 * Read Data
	 * 
	 * @param fileName
	 * @throws Exception
	 */
	public static void read(String fileName) throws Exception {
		
		//get filesSystem
		FileSystem fileSystem = getFileSystem();
		
		// get path
		Path readPath = new Path(fileName);
		// open file
		FSDataInputStream inputStream = fileSystem.open(readPath);
		
		try {
		    // read file content
		    IOUtils.copyBytes(inputStream, System.out, 4096, false);
		} catch (Exception e) {
		    e.printStackTrace();
		} finally {
		    IOUtils.closeStream(inputStream);
		}
		
	}
	
	public static void write() throws Exception {
	
		//get fileSystem
		FileSystem fileSystem = getFileSystem();
		
		//write path
		String putFilename = "/user/root/put-wc.input";
		Path writePath = new Path(putFilename);
		
		//out Stream
		FSDataOutputStream outputSummer = fileSystem.create(writePath);
		
		//flie input stream
		FileInputStream inputStream = new FileInputStream(new File(
						"/opt/modules/hadoop-2.5.0-cdh5.3.6/wc.input"));
		
		//stream read/write
		try {
		    // read file content
		    IOUtils.copyBytes(inputStream, outputSummer, 4096, false);
		} catch (Exception e) {
		    e.printStackTrace();
		} finally {
		    IOUtils.closeStream(inputStream);
		    IOUtils.closeStream(outputSummer);
		}
		
	}
    
    public static void main(String[] args) throws Exception {
		String fileName = "/user/sun/mapreduce/wordcount/input/wc.input";
		read(fileName);
	
		// write native file to hdfs
		write();
		
    	}
}	

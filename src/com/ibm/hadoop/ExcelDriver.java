package com.ibm.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ExcelDriver {
	
	public static void main(String args[]) throws ClassNotFoundException, InterruptedException{
		try {
			Job job = new Job();
			job.setJarByClass(ExcelDriver.class);
			job.setJobName("Excel Record Reader");
			job.setMapperClass(ExcelMapper.class);
			job.setNumReduceTasks(0);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setInputFormatClass(ExcelInputFormat.class);
			System.exit(job.waitForCompletion(true)? 0: -1);
						
		} catch (IOException ie) {
			ie.printStackTrace();
		}
		
	}
}

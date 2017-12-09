package com.ibm.hadoop;

import java.io.IOException;
import java.io.InputStream;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ExcelRecordReader extends RecordReader<LongWritable, Text>{

	private LongWritable key;
	private Text value;
	private InputStream is;
	private String[] records;
	
	@Override
	public void close() throws IOException {
		if(is != null)
		is.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		FileSplit fileSplit = (FileSplit)split;
		Configuration job = context.getConfiguration();
		final Path file = fileSplit.getPath();
		
		
		FileSystem fileSystem = file.getFileSystem(job);
		FSDataInputStream fileIn = fileSystem.open(fileSplit.getPath());
		
		is = fileIn;
		String line;
		try {
			line = new ExcelParser().parseExcellData(is);
			this.records = line.split("\n");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
			
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(key == null){
			key = new LongWritable(0);
			value = new Text(records[0]);
		}else{
			if(key.get() < (this.records.length - 1)){
				long pos = (int) key.get(); 
				key.set(pos + 1);
				value.set(this.records[(int) (pos + 1)]);
				pos++;
			}else{
				return false;
			}
		}
		
		if(key == null || value == null){
			return false;
		}else{
			return true;
		}
	}

}

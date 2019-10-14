package com.fsnip.bigdata.mapreduce.defineinputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class AuthReader extends RecordReader<IntWritable, Text> {
	
	private FileSplit fs;
	private LineReader reader;
	private IntWritable key;
	private Text value;
	
	int count = 0;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fs = (FileSplit) split;
		Path path = fs.getPath();
		//获取文件系统
		Configuration conf = context.getConfiguration();
		FileSystem fileSystem = path.getFileSystem(conf);
		//获取指定路径的文件流
		FSDataInputStream in = fileSystem.open(path);
		reader = new LineReader(in);
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		key = new IntWritable();
		value = new Text();
		Text temp = new Text();
		int len = reader.readLine(temp);
		if(len == 0){
			return false;
		}else{
			value.append(temp.getBytes(), 0, temp.getLength());
			count++;
			key.set(count);
			return true;
		}
	}

	@Override
	public void close() throws IOException {
		if(reader != null){
			reader.close();
		}
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
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


}

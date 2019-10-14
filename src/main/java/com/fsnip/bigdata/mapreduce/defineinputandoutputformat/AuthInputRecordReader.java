package com.fsnip.bigdata.mapreduce.defineinputandoutputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class AuthInputRecordReader extends RecordReader<Text, Text> {
	
	private FileSplit fs;
	private Text key;
	private Text value;
	private LineReader reader;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fs = (FileSplit) split;
		Path path = fs.getPath();
		Configuration conf = context.getConfiguration();
		FileSystem fileSystem = path.getFileSystem(conf);
		FSDataInputStream in = fileSystem.open(path);
		reader = new LineReader(in);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		key = new Text();
		value = new Text();
		Text temp = new Text();
		int len = reader.readLine(temp);
		if(len == 0){
			return false;
		}else{
			key.set(temp);
			for(int i=0; i<2; i++){
				reader.readLine(temp);
				value.append(temp.getBytes(), 0, temp.getLength());
				value.append(" ".getBytes(), 0, 1);
			}
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
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return new Text(value.toString().trim());
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		
		return 0;
	}

}

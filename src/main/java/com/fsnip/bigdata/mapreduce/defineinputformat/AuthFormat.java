package com.fsnip.bigdata.mapreduce.defineinputformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class AuthFormat extends FileInputFormat<IntWritable, Text>{

	@Override
	public RecordReader<IntWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		return new AuthReader();
	}

}

package com.fsnip.bigdata.mapreduce.defineinputandoutputformat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class AuthoInputFormat extends FileInputFormat<Text, Text>{

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new AuthInputRecordReader();
	}

}

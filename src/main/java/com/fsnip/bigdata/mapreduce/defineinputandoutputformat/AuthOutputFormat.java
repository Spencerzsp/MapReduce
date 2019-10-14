package com.fsnip.bigdata.mapreduce.defineinputandoutputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AuthOutputFormat<K, V> extends FileOutputFormat<K, V>{

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		Path path = super.getDefaultWorkFile(context, "");
		Configuration conf = context.getConfiguration();
		FileSystem system = path.getFileSystem(conf);
		FSDataOutputStream out = system.create(path);
		return new AuthOutputRecordWriter<K, V>(out);
	}


}

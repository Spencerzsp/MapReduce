package com.fsnip.bigdata.mapreduce.defineinputandoutputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class AuthOutputRecordWriter<K, V> extends RecordWriter<K, V> {
	
	private FSDataOutputStream out;
	public AuthOutputRecordWriter(FSDataOutputStream out) {
		this.out = out;
	}
	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		out.close();
	}

	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		out.write(key.toString().getBytes());
		out.write("***".getBytes());
		out.write(value.toString().getBytes());
		out.write("\r\n".getBytes());
	}

}

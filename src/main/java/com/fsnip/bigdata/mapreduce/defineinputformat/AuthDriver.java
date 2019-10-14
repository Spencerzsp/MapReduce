package com.fsnip.bigdata.mapreduce.defineinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AuthDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(AuthDriver.class);
		job.setMapperClass(AuthMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(AuthFormat.class);

		FileInputFormat.setInputPaths(job,new Path("hdfs://hadoop01:9000/test/word/word.txt"));
		FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop01:9000/test/word/defineinputformat"));
		job.waitForCompletion(true);
	}

}

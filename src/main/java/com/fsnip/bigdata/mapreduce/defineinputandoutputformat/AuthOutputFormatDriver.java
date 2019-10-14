package com.fsnip.bigdata.mapreduce.defineinputandoutputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AuthOutputFormatDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(AuthOutputFormatDriver.class);
		job.setMapperClass(AuthOutputFormatMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(AuthoInputFormat.class);
		job.setOutputFormatClass(AuthOutputFormat.class);

		FileInputFormat.setInputPaths(job,new Path("hdfs://hadoop01:9000/test/auth_format/input_output.txt"));
		FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop01:9000/test/auth_format/result"));
		job.waitForCompletion(true);
	}

}

package com.fsnip.bigdata.mapreduce.yesun;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class YesunDriver {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(YesunDriver.class);
		job.setMapperClass(YesunMapper.class);
		job.setReducerClass(YesunReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);     

		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop01:9000/test/yesun/yesun.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/test/yesun/yesun_relation"));

		job.waitForCompletion(true);

	}

}

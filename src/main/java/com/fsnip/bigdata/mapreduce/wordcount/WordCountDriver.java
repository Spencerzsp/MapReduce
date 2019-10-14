package com.fsnip.bigdata.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(WordCountDriver.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("file:///E:\\big-workspace\\Hadoop01\\src\\main\\java\\com\\fsnip\\bigdata\\mapreduce\\wordcount\\data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("file:///E:\\big-workspace\\Hadoop01\\src\\main\\java\\com\\fsnip\\bigdata\\mapreduce\\wordcount\\data.txt\\wordcount"));
		
		job.waitForCompletion(true);
		
	}

}

package com.fsnip.bigdata.mapreduce.moviesort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MovieSortDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MovieSortDriver.class);
		job.setMapperClass(MovieSortMapper.class);
		
		job.setMapOutputKeyClass(MovieBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop01:9000/test/movie/movie.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/test/movie/moviesort"));
		
		job.waitForCompletion(true);
	}

}

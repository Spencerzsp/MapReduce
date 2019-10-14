package com.fsnip.bigdata.mapreduce.flowcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCountDriver {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowCountDriver.class);
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		job.setNumReduceTasks(3);
		job.setPartitionerClass(FlowCountPartitioner.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop01:9000/test/flow/flow.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/test/flow/flowcount"));
		
		job.waitForCompletion(true);
	}

}

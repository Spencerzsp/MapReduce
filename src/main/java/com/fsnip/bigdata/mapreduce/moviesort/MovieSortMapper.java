package com.fsnip.bigdata.mapreduce.moviesort;

import java.io.IOException;

import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieSortMapper extends Mapper<LongWritable, Text, MovieBean, NullWritable>{
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, MovieBean, NullWritable>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String name = line.split(" ")[0];
		int score = Integer.parseInt(line.split(" ")[1]);
		
		MovieBean movieBean = new MovieBean();
		movieBean.setName(name);
		movieBean.setScore(score);
		
		context.write(movieBean, NullWritable.get());
		
	}
}

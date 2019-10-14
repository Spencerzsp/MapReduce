package com.fsnip.bigdata.mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JoinMapper extends Mapper<LongWritable, Text, Text, Item>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Item>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] info = line.split(" ");
		Item item = new Item();
		FileSplit split = (FileSplit) context.getInputSplit();
		if(split.getPath().getName().startsWith("order")){
			item.setId(info[0]);
			item.setDate(info[1]);
			item.setPid(info[2]);
			item.setAmount(Integer.parseInt(info[3]));
			item.setName("");
			item.setPrice(0.0);
		}else{
			item.setPid(info[0]);
			item.setName(info[1]);
			item.setPrice(Double.parseDouble(info[2]));
			item.setId("");
			item.setDate("");
			item.setAmount(0);
		}
		context.write(new Text(item.getPid()), item);
	}
}

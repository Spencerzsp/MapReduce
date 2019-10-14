package com.fsnip.bigdata.mapreduce.flowcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] info = line.split("\t");
		FlowBean flowBean = new FlowBean();
		flowBean.setPhone(info[0]);
		flowBean.setName(info[1]);
		flowBean.setAddr(info[2]);
		flowBean.setFlow(Integer.parseInt(info[3]));
		
		context.write(new Text(flowBean.getName()), flowBean);
	}
}

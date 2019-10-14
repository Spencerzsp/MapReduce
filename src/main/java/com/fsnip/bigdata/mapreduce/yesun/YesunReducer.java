package com.fsnip.bigdata.mapreduce.yesun;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class YesunReducer extends Reducer<Text, Text, Text, NullWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {

		List<String> grandparList = new ArrayList<String>();
		List<String> grandsonList = new ArrayList<String>();
		for (Text value : values) {
			if(value.toString().startsWith("+")){
				grandparList.add(value.toString().substring(1));
			}
			else{
				grandsonList.add(value.toString().substring(1));
			}
		}
		if(grandparList.size()>0 && grandsonList.size()>0){
			String relation = "爷爷:" + grandparList + "-->" + "孙子:" + grandsonList;
			context.write(new Text(relation), NullWritable.get());
		}
	}

}

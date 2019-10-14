package com.fsnip.bigdata.mapreduce.flowcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowCountPartitioner extends Partitioner<Text, FlowBean>{

	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		if(value.getAddr().equals("beijing")){
			return 0;
		}else if(value.getAddr().equals("shanghai")){
			return 1;
		}else if(value.getAddr().equals("chengdu")){
			return 2;
		}else{
			return 0;
		}		
	}
	
}

package com.fsnip.bigdata.mapreduce.flowcount;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
	
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {

		FlowBean flowBean = new FlowBean();
		for (FlowBean value : values) {
			flowBean.setPhone(value.getPhone());
			flowBean.setName(value.getName());
			flowBean.setAddr(value.getAddr());
			if(flowBean.getFlow() != null){
				flowBean.setFlow(flowBean.getFlow()+value.getFlow());
			}else{
				flowBean.setFlow(value.getFlow());
			}
		}
		context.write(key, flowBean);
	}

}

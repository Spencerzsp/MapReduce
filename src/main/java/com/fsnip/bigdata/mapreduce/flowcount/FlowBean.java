package com.fsnip.bigdata.mapreduce.flowcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{
	
	private String phone;
	private String name;
	private String addr;
	private Integer flow;

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAddr() {
		return addr;
	}

	public void setAddr(String addr) {
		this.addr = addr;
	}

	public Integer getFlow() {
		return flow;
	}

	public void setFlow(Integer flow) {
		this.flow = flow;
	}
	
	@Override
	public String toString() {
		return "FlowBean [phone=" + phone + ", name=" + name + ", addr=" + addr + ", flow=" + flow + "]";
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(phone);
		out.writeUTF(name);
		out.writeUTF(addr);
		out.writeInt(flow);
	}

	public void readFields(DataInput in) throws IOException {
		this.phone = in.readUTF();
		this.name = in.readUTF();
		this.addr = in.readUTF();
		this.flow = in.readInt();
		
	}
}

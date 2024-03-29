package com.fsnip.bigdata.mapreduce.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Item implements Writable,Cloneable{
	
	private String id = "";
	private String date = "";
	private String pid = "";
	private Integer amount ;
	private String name = "";
	private Double price;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public Integer getAmount() {
		return amount;
	}

	public void setAmount(Integer amount) {
		this.amount = amount;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}
	
	public Item clone(){
		Item o=null;
		try {
			o=(Item)super.clone();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return o;
	}

	@Override
	public String toString() {
		return "Item [订单=" + id + ", 订单日期=" + date + ", 物品id=" + pid + ", 出货量=" + amount + ", 品牌=" + name
				+ ", 商品单价=" + price + "]";
	}

	public void readFields(DataInput in) throws IOException {
		
		this.id = in.readUTF();
		this.date = in.readUTF();
		this.pid = in.readUTF();
		this.amount = in.readInt();
		this.name = in.readUTF();
		this.price = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(id);
		out.writeUTF(date);
		out.writeUTF(pid);
		out.writeInt(amount);
		out.writeUTF(name);
		out.writeDouble(price);
	}

}

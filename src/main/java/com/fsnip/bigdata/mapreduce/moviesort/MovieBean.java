package com.fsnip.bigdata.mapreduce.moviesort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MovieBean implements WritableComparable<MovieBean>{
	
	private String name;
	private Integer score;
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getScore() {
		return score;
	}

	public void setScore(Integer score) {
		this.score = score;
	}

	@Override
	public String toString() {
		return "MovieBean [name=" + name + ", score=" + score + "]";
	}

	public void readFields(DataInput in) throws IOException {

		this.name = in.readUTF();
		this.score = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(score);
	}

	public int compareTo(MovieBean o) {
		
		return this.score - o.score;
	}

}

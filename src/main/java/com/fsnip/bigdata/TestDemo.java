package com.fsnip.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;


public class TestDemo {
	
	@Test
	public void connect() throws Exception{
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "1");
		
		//连接分布式文件系统
		FileSystem fs = FileSystem.get(new URI(
				"hdfs://hadoop01:9000"), conf);
		fs.close();
	}
	
	@Test
	public void get() throws Exception{
		Configuration conf = new Configuration();
		//连接分布式文件系统
		FileSystem fs = FileSystem.get(new URI(
				"hdfs://hadoop01:9000"), conf);
		FSDataInputStream in = fs.open(new Path("/user/hive/warehouse/test.db/2.txt"));
		OutputStream out = new FileOutputStream(new File("data.txt"));
		
		//通过hadoop工具类完成数据的传输
		IOUtils.copyBytes(in, out, conf);
		in.close();
		out.close();
		fs.close();
	}

}

package cludcourse.template;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<Object, Text, Text, IntWritable> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	
	}
}

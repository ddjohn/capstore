package cloudcourse.g1q1;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<Object, Text, Text, IntWritable> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().replace('(', ' ').replace(')',' ').trim().split(",");

		if(tokens.length == 2) {
			String origin = tokens[0];
			String dest = tokens[1];
			
			context.write(new Text(origin), new IntWritable(1));
			context.write(new Text(dest), new IntWritable(1));			
		}
	}
}

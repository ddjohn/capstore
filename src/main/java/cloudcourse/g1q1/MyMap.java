package cloudcourse.g1q1;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<Object, Text, Text, IntWritable> {
	private Text word = new Text();
	private final static IntWritable valueOne = new IntWritable(1);
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		
		String line = value.toString();
		String tokens[] = line.substring(1, line.length() - 1).split(",");

		if(tokens.length == 2) {
			word.set(tokens[0]); // origin 
			context.write(word, valueOne);
			
			word.set(tokens[1]); // destination
			context.write(word, valueOne);			
		}
		else {
			System.out.println("Discarding: " + line);
		}
	}
}

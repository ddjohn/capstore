package cloudcourse.g1q2;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<Object, Text, Text, FloatWritable> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String tokens[] = line.substring(1, line.length() - 1).split(",");

		if(tokens.length == 3) {
			String company = tokens[0];
			String     day = tokens[1];
			String   delay = tokens[2];
			context.write(new Text(company), new FloatWritable(Float.parseFloat(delay)));
		} 
		else {
			System.out.println("Discarding: " + line);
		}
	}
}

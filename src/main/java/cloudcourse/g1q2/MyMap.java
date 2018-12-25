package cloudcourse.g1q2;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<Object, Text, Text, FloatWritable> {
	private Text company = new Text();
	private FloatWritable delay = new FloatWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String tokens[] = line.substring(1, line.length() - 1).split(",");

		if(tokens.length == 3) {
			company.set(tokens[0]);
			delay.set(Float.parseFloat(tokens[2]));
			context.write(company, delay);
		} 
		else {
			System.out.println("Discarding: " + line);
		}
	}
}

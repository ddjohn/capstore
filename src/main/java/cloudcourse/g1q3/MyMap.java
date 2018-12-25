package cloudcourse.g1q3;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<Object, Text, Text, FloatWritable> {
	private Text dayofweek = new Text();
	private FloatWritable delay = new FloatWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String tokens[] = line.substring(1, line.length() - 1).split(",");

		if(tokens.length == 3) {
			dayofweek.set(tokens[1]);
			delay.set(Float.parseFloat(tokens[2]));
			context.write(dayofweek, delay);
		} 
		else {
			System.out.println("Discarding: " + line);
		}
	}
}

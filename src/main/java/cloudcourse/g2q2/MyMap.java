package cloudcourse.g2q2;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import cloudcourse.globals.DataSet;

public class MyMap extends Mapper<Object, Text, Text, FloatWritable> {
	private Text combo = new Text();
	private FloatWritable delay = new FloatWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String tokens[] = line.substring(1, line.length() - 1).split(",");

		if(tokens.length >=8 && 
				tokens[DataSet.ORIGIN].isEmpty() == false && 
				tokens[DataSet.ORIGIN].isEmpty() == false && 
				tokens[DataSet.DEPDELAY].isEmpty() == false) {
			
			combo.set(tokens[DataSet.ORIGIN] + "_" + tokens[DataSet.ORIGIN]);
			delay.set(Float.parseFloat(tokens[DataSet.DEPDELAY]));
			context.write(combo, delay);
		} 
		else {
			System.out.println("Discarding: " + line);
			DataSet.discarded++;
		}
	}
}

package cloudcourse.g2q3;

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

		if(tokens.length >=10 && 
				tokens[DataSet.AIRLINEID].isEmpty() == false && 
				tokens[DataSet.ORIGIN].isEmpty() == false && 
				tokens[2].isEmpty() == false && 
				tokens[9].isEmpty() == false) {
			
			combo.set(tokens[DataSet.ORIGIN] + "_" + tokens[DataSet.DEST] + "_" + tokens[DataSet.AIRLINEID]);
			delay.set(Float.parseFloat(tokens[DataSet.ARRDELAY]));
			context.write(combo, delay);
		} 
		else {
			System.out.println("Discarding: " + line);
			DataSet.discarded++;
		}
	}
}

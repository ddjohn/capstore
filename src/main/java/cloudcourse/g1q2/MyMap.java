package cloudcourse.g1q2;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import cloudcourse.globals.DataSet;

public class MyMap extends Mapper<Object, Text, Text, FloatWritable> {
	private Text carrier = new Text();
	private FloatWritable delay = new FloatWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String tokens[] = line.substring(1, line.length() - 1).split(",");

		if(tokens.length > DataSet.ARRDELAY && 
				tokens[DataSet.UNIQUECARRIER].isEmpty() == false && 
				tokens[DataSet.ARRDELAY].isEmpty() == false) {
			
			carrier.set(tokens[DataSet.UNIQUECARRIER]);
			delay.set(Float.parseFloat(tokens[DataSet.ARRDELAY]));
			context.write(carrier, delay);
		} 
		else {
//			System.out.println("Discarding: " + line);
			DataSet.discarded++;	
		}
	}
}

package mapreduce.g1q3;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import mapreduce.globals.DataSet;

public class MyMap extends Mapper<Object, Text, Text, FloatWritable> {
	private Text dayofweek = new Text();
	private FloatWritable delay = new FloatWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String tokens[] = line.substring(1, line.length() - 1).split(",");

		if(tokens.length > DataSet.ARRDELAY && 
				tokens[DataSet.DAYOFWEEK].isEmpty() == false && 
				tokens[DataSet.ARRDELAY].isEmpty() == false) {
			
			dayofweek.set(tokens[DataSet.DAYOFWEEK]);
			delay.set(Float.parseFloat(tokens[DataSet.ARRDELAY]));
			context.write(dayofweek, delay);
		} 
		else {
			//System.out.println("Discarding: " + line);
			DataSet.discarded++;
		}
	}
}

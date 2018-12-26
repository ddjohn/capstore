package cloudcourse.g2q1;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<Object, Text, Text, FloatWritable> {
	private Text combo = new Text();
	private FloatWritable delay = new FloatWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String tokens[] = line.substring(1, line.length() - 1).split(",");

		//ORIGIN, DEST, UNIQUECARRIER, DEPDELAY, ARRDELAY
//		if(tokens.length == 5) {
		if(tokens.length > 3 && tokens[3] != null) {
			combo.set(tokens[0] + "_" + tokens[2]);
			delay.set(Float.parseFloat(tokens[3]));
			context.write(combo, delay);
		} 
		else {
			System.out.println("Discarding: " + line);
		}
	}
}

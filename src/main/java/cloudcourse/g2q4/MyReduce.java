package cloudcourse.g2q4;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	private FloatWritable averageSum = new FloatWritable();
	
	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
		System.out.println("----");

		float sum = 0;
		int count = 0;
		for (FloatWritable val : values) {
			System.out.println(key+ "," + val);
			sum += val.get();
			count++;
		}
		
		averageSum.set(sum / count);
		context.write(key, averageSum);
	}
}

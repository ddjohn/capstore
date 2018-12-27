package cloudcourse.g1q2;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	private FloatWritable averageValue = new FloatWritable();
	
	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

		float sum = 0;
		int count = 0;
		for (FloatWritable val : values) {
			sum += val.get();
			count++;
		}
		
		averageValue.set(sum / count);
		context.write(key, averageValue);
	}
}

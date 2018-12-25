package cloudcourse.g1q2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyMain extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(this.getConf(), "MyJob");

		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		job.setCombinerClass(MyCombiner.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		FileInputFormat.setInputPaths(job, new Path("/input"));
		FileOutputFormat.setOutputPath(job, new Path("/output"));

		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Starting...");
		int res = ToolRunner.run(new Configuration(), new MyMain(), args);
		System.out.println("Ending...");
		System.exit(res);
	}
}

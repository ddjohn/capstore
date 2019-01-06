package spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;
import cloudcourse.globals.DataSet;

public class G2Q2Main {

	public static final void main(String[] args) throws InterruptedException {
		
		SparkConf conf = new SparkConf().setAppName( "My application");
		SparkContext sc = new SparkContext(conf);
		//JavaRDDstring cassandraRdd = CassandraJavaUtil.javaFunctions(sc)
		 //       .cassandraTable("my_keyspace", "my_table", .mapColumnTo(String.class))
		  //      .select("my_column");
		
		
		MyContext ctx = new MyContext();

		ctx.createStream("cloudcourse")

		.flatMapToPair(x -> {
			String[] tokens =  x.value().substring(1, x.value().length() - 1).split(",");

			List<Tuple2<String, Float>> list = new ArrayList<Tuple2<String, Float>>();
			if(tokens.length > DataSet.DEPDELAY && 
					tokens[DataSet.ORIGIN].isEmpty() == false && 
					tokens[DataSet.DEST].isEmpty() == false && 
					tokens[DataSet.DEPDELAY].isEmpty() == false) {

				list.add(new Tuple2<String, Float>(tokens[DataSet.ORIGIN] + "_" + tokens[DataSet.DEST], Float.parseFloat(tokens[DataSet.DEPDELAY])));
			}
			return list.iterator();
		})

		// Remember the keys 
		.updateStateByKey((nums, current) -> {

			Average average = current.or(new Average());
			for(float i : nums) {
				average.count++;
				average.sum += i;
			}
			return Optional.of(average);
		})

		// Sort by swapping values to keys and back
		.mapToPair(x -> x.swap())
		.transformToPair(x -> x.sortByKey(true))
		.mapToPair(x -> x.swap())

		// Print top 10
		.print(Integer.MAX_VALUE);

		ctx.run();
		ctx.close();
	}
}

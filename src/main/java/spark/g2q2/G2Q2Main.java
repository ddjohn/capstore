package spark.g2q2;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import mapreduce.globals.DataSet;
import scala.Tuple2;
import spark.globals.Average;
import spark.globals.MyContext;

public class G2Q2Main {
	private static final String[] FILTER = {"SRQ", "CMH", "JFK", "SEA", "BOS"};

	public static final void main(String[] args) throws InterruptedException {

		MyContext ctx = new MyContext();

		JavaPairDStream<String, Average> stream = ctx.createStream("cloudcourse")

				// Parse input data
				.flatMapToPair(x -> {
					String[] tokens =  x.value().substring(1, x.value().length() - 1).split(",");

					// Build list with (origin_dest, depdelay)
					List<Tuple2<String, Float>> list = new ArrayList<Tuple2<String, Float>>();
					if(tokens.length > DataSet.DEPDELAY && 
							tokens[DataSet.ORIGIN  ].isEmpty() == false && 
							tokens[DataSet.DEST    ].isEmpty() == false && 
							tokens[DataSet.DEPDELAY].isEmpty() == false) {

						list.add(new Tuple2<String, Float>(tokens[DataSet.ORIGIN] + "_" + tokens[DataSet.DEST], Float.parseFloat(tokens[DataSet.DEPDELAY])));
					}
					return list.iterator();
				})

				// Update the average class with batch information 
				.updateStateByKey((nums, current) -> {

					Average average = current.or(new Average());
					for(float i : nums) {
						average.count++;
						average.sum += i;
					}
					return Optional.of(average);
				});

		// Filter out fields of interest
		stream
		.filter(x -> {
			for(String f : FILTER) {
				if(x._1.startsWith(f)) {
					return true;
				}
			}
			return false;
		})

		// Sort by swapping values to keys and back
		.transformToPair(x -> x.sortByKey(true))

		// Print
		.print(1000);

		// Save to Cassandra
		stream
		.map(x -> {
			String[] tokens = x._1.split("_");
			return new G2Q2Database(tokens[0], tokens[1], x._2.average());
		})

		.foreachRDD(rdd -> {
			CassandraJavaUtil.javaFunctions(rdd).writerBuilder(
					"cloudcourse", "g2q2", 
					CassandraJavaUtil.mapToRow(G2Q2Database.class))
			.saveToCassandra();
		});

		ctx.run();
		ctx.close();
	}
}

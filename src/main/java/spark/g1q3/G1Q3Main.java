package spark.g1q3;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import scala.Tuple2;
import spark.globals.Average;
import spark.globals.MyContext;
import cloudcourse.globals.DataSet;

public class G1Q3Main {

	public static final void main(String[] args) throws InterruptedException {

		MyContext ctx = new MyContext();

		JavaPairDStream<String, Average> stream = ctx.createStream("cloudcourse")

				.flatMapToPair(x -> {

					// Parse input data
					String[] tokens =  x.value().substring(1, x.value().length() - 1).split(",");

					// Build list with (day, arrdelay)
					List<Tuple2<String, Float>> list = new ArrayList<Tuple2<String, Float>>();
					if(tokens.length > DataSet.ARRDELAY && 
							tokens[DataSet.DAYOFWEEK].isEmpty() == false && 
							tokens[DataSet.ARRDELAY ].isEmpty() == false) {

						list.add(new Tuple2<String, Float>(tokens[DataSet.DAYOFWEEK], Float.parseFloat(tokens[DataSet.ARRDELAY])));
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

		// Sort by swapping values to keys and back
		stream
		.mapToPair(x -> x.swap())
		.transformToPair(x -> x.sortByKey(true))
		.mapToPair(x -> x.swap())

		// Print top 10
		.print();

		// Save to Cassandra
		stream
		.map(x -> new G1Q3Database(x._1, x._2.average()))

		.foreachRDD(rdd -> {
			CassandraJavaUtil.javaFunctions(rdd).writerBuilder(
					"cloudcourse", "g1q3", 
					CassandraJavaUtil.mapToRow(G1Q3Database.class))
			.saveToCassandra();
		});

		ctx.run();
		ctx.close();
	}
}
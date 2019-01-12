package spark.g1q1;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import cloudcourse.globals.DataSet;
import scala.Tuple2;
import spark.globals.MyContext;

public class G1Q1Main {

	public static final void main(String[] args) throws InterruptedException {
		
		MyContext ctx = new MyContext();

		JavaPairDStream<String, Long> stream = ctx.createStream("cloudcourse")
				
				.flatMapToPair(x -> {
					
					// Parse the input data
					String[] tokens =  x.value().substring(1, x.value().length() - 1).split(",");

					// Build a list with (DEST,1) and (ORIGIN,1)
					List<Tuple2<String, Long>> list = new ArrayList<Tuple2<String, Long>>();
					if(tokens.length > DataSet.ORIGIN &&
							tokens[DataSet.ORIGIN].isEmpty() == false &&
							tokens[DataSet.DEST  ].isEmpty() == false) {

						list.add(new Tuple2<String, Long>(tokens[DataSet.ORIGIN], 1L));
						list.add(new Tuple2<String, Long>(tokens[DataSet.DEST], 1L));
					}
					return list.iterator();
				})

				// Sum by key
				.reduceByKey((i1, i2) -> i1 + i2)

				// Remember the keys
				.updateStateByKey((nums, current) -> {
					long sum = current.or(0L); 
					for(long i : nums) {
						sum += i;
					}
					return Optional.of(sum);
				});

		// Sort by swapping values to keys and back
		stream		
		.mapToPair(x -> x.swap())
		.transformToPair(x -> x.sortByKey(false))
		.mapToPair(x -> x.swap())
		
		// Print top 10
		.print(10);

		// Save to Cassandra
		stream
		.map(x -> new G1Q1Database(x._1, x._2))

		.foreachRDD(rdd -> {
			CassandraJavaUtil.javaFunctions(rdd).writerBuilder(
					"cloudcourse", "g1q1", 
					CassandraJavaUtil.mapToRow(G1Q1Database.class))
			.saveToCassandra();
		});

		ctx.run();
		ctx.close();
	}
}
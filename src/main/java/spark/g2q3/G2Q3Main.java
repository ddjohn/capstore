package spark.g2q3;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import scala.Tuple2;
import spark.globals.Average;
import spark.globals.MyContext;
import cloudcourse.globals.DataSet;

public class G2Q3Main {
	private static final String[] FILTER = {"CMI_ORD", "IND_CMH", "DFW_IAH", "LAX_SFO", "JFK_LAX", "ATL_PHX"};

	public static final void main(String[] args) throws InterruptedException {

		MyContext ctx = new MyContext();

		JavaPairDStream<String, Average> stream = ctx.createStream("cloudcourse")

				.flatMapToPair(x -> {

					// Parse input data
					String[] tokens =  x.value().substring(1, x.value().length() - 1).split(",");

					// Build list with (carrier_origin_dest, arrdelay)
					List<Tuple2<String, Float>> list = new ArrayList<Tuple2<String, Float>>();
					if(tokens.length > DataSet.ARRDELAY && 
							tokens[DataSet.UNIQUECARRIER].isEmpty() == false && 
							tokens[DataSet.ORIGIN       ].isEmpty() == false && 
							tokens[DataSet.DEST         ].isEmpty() == false && 
							tokens[DataSet.ARRDELAY     ].isEmpty() == false) {

							list.add(new Tuple2<String, Float>(tokens[DataSet.ORIGIN] + "_" + tokens[DataSet.DEST] + "_" + tokens[DataSet.UNIQUECARRIER], Float.parseFloat(tokens[DataSet.ARRDELAY])));
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

		// Filter out the combinations of interest
		stream
		.filter(x -> {
			for(String f : FILTER) {
				if(x._1.startsWith(f)) {
					return true;
				}
			}
			return false;
		})

		// Sort
		.transformToPair(x -> x.sortByKey(true))

		// Print
		.print(1000);


		// Save to cassandra
		stream
		.map(x -> {
			String[] tokens = x._1.split("_");
			return new G2Q3Database(tokens[0], tokens[1], tokens[2], x._2.average());
		})

		.foreachRDD(rdd -> {
			CassandraJavaUtil.javaFunctions(rdd).writerBuilder(
					"cloudcourse", "g2q3", 
					CassandraJavaUtil.mapToRow(G2Q3Database.class))
			.saveToCassandra();
		});

		ctx.run();
		ctx.close();
	}
}

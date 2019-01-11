package spark.g2q2;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.Optional;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import scala.Tuple2;
import spark.globals.Average;
import spark.globals.MyContext;
import cloudcourse.globals.DataSet;

public class G2Q2Main {

	public static final void main(String[] args) throws InterruptedException {

		MyContext ctx = new MyContext();

		ctx.createStream("cloudcourse")

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
		})

		// Sort by swapping values to keys and back
		.mapToPair(x -> x.swap())
		.transformToPair(x -> x.sortByKey(true))
		.mapToPair(x -> x.swap())

		.map(x -> {
			String[] tokens = x._1.split("_");
			return new G2Q2Database(tokens[0], tokens[1], x._2.average());
		})

		.foreachRDD(rdd -> {

			CassandraJavaUtil.javaFunctions(rdd).writerBuilder(
					"cloudcourse", 
					"g2q2", 
					CassandraJavaUtil.mapToRow(G2Q2Database.class))
			.saveToCassandra();
		});

		// Print
		//.print(1000);

		ctx.run();
		ctx.close();
	}
}

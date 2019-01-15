package spark.g3q2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.streaming.api.java.JavaDStream;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import mapreduce.globals.DataSet;
import spark.globals.MyContext;

public class G3Q2Main implements Serializable {
	private static final long serialVersionUID = 1L;

	private String[] FILTER_ORIGIN = {"BOS", "PHX", "DFW", "LAX"};
	private String[] FILTER_LEG    = {"ATL", "JFK", "STL", "MIA"};
	private String[] FILTER_DEST   = {"LAX", "MSP", "ORD", "LAX"};

	public G3Q2Main() throws InterruptedException {

		MyContext ctx = new MyContext();

		JavaDStream<TomsFlight> stream = ctx.createStream("cloudcourse")

				.flatMap(x -> {
					String[] tokens = x.value().substring(1, x.value().length() - 1).split(",");

					List<TomsFlight> list = new ArrayList<TomsFlight>();

					if(tokens.length > DataSet.ARRDELAY) {
						list.add(new TomsFlight(
								tokens[DataSet.ORIGIN], 
								tokens[DataSet.DEST], 
								tokens[DataSet.FLIGHTDATE],
								tokens[DataSet.DEPTIME],
								tokens[DataSet.UNIQUECARRIER] + " " + tokens[DataSet.FLIGHTNUM],
								Float.parseFloat(tokens[DataSet.ARRDELAY])));
					}
					return list.iterator();
				})

				// Limit to flights from 2008
				.filter(x -> x.depDate.startsWith("2008"))

				.filter(x -> {
					if("1200".compareTo(x.depTime) >= 0 && Arrays.asList(FILTER_ORIGIN).contains(x.origin) && Arrays.asList(FILTER_LEG).contains(x.dest) ) {
						return true;
					}
					if("1200".compareTo(x.depTime) <= 0 && Arrays.asList(FILTER_LEG).contains(x.origin) && Arrays.asList(FILTER_DEST).contains(x.dest) ) {
						return true;
					}
					return false;
				});

		// Save to Cassandra
		stream
		.foreachRDD(rdd -> {
			CassandraJavaUtil.javaFunctions(rdd).writerBuilder(
					"cloudcourse", "g3q2", 
					CassandraJavaUtil.mapToRow(TomsFlight.class))
			.saveToCassandra();
		});

		// Print progress
		stream
		.print();

		ctx.run();
		ctx.close();
	}

	public static void main(String[] args) throws InterruptedException {
		new G3Q2Main();
	}
}
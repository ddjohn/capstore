package spark;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import scala.Tuple2;
import cloudcourse.globals.DataSet;

public class G3Q2Main {

	public G3Q2Main(String origin, String middle, String dest, String originDate, String middleDate) throws InterruptedException {

		MyContext ctx = new MyContext();

		JavaInputDStream<ConsumerRecord<String, String>> stream = ctx.createStream("cloudcourse");

		JavaPairDStream<String, TomsFlight> part1 = stream
				.flatMapToPair(x -> {
					List<Tuple2<String, TomsFlight>> list = new ArrayList<Tuple2<String, TomsFlight>>();

					String[] tokens = x.value().substring(1, x.value().length() - 1).split(",");
					if(tokens[DataSet.FLIGHTDATE] == originDate) {
						return list.iterator(); 
					}

					if(tokens.length > DataSet.ARRDELAY) {
						try {
							list.add(new Tuple2<String, TomsFlight>(
									tokens[DataSet.DEST],
									new TomsFlight(
											tokens[DataSet.ORIGIN], 
											tokens[DataSet.DEST], 
											tokens[DataSet.FLIGHTDATE],
											tokens[DataSet.DEPTIME],
											tokens[DataSet.UNIQUECARRIER] + " " + tokens[DataSet.FLIGHTNUM],
											Float.parseFloat(tokens[DataSet.ARRDELAY]))));
						}
						catch(Exception e) {
							System.out.println("e: " + e);
						}
					}
					return list.iterator();
				})
				.filter(x ->     origin.compareTo(x._2.origin ) == 0)
				.filter(x ->     middle.compareTo(x._2.dest   ) == 0)
				.filter(x -> originDate.compareTo(x._2.depDate) == 0)
				.filter(x ->     "1200".compareTo(x._2.depTime) >= 0);

		JavaPairDStream<String, TomsFlight> part2 = stream
				.flatMapToPair(x -> {
					List<Tuple2<String, TomsFlight>> list = new ArrayList<Tuple2<String, TomsFlight>>();

					String[] tokens = x.value().substring(1, x.value().length() - 1).split(",");
					if(tokens[DataSet.FLIGHTDATE] == middleDate) {
						return list.iterator(); 
					}

					if(tokens.length > DataSet.ARRDELAY) {
						try {
							list.add(new Tuple2<String, TomsFlight>(
									tokens[DataSet.ORIGIN],
									new TomsFlight(
											tokens[DataSet.ORIGIN], 
											tokens[DataSet.DEST], 
											tokens[DataSet.FLIGHTDATE],
											tokens[DataSet.DEPTIME],
											tokens[DataSet.UNIQUECARRIER] + " " + tokens[DataSet.FLIGHTNUM],
											Float.parseFloat(tokens[DataSet.ARRDELAY]))));
						}
						catch(Exception e) {
							System.out.println("e: " + e);
						}
					}
					return list.iterator();
				})
				.filter(x ->     middle.compareTo(x._2.origin ) == 0)
				.filter(x ->       dest.compareTo(x._2.dest   ) == 0)
				.filter(x -> middleDate.compareTo(x._2.depDate) == 0)
				.filter(x ->     "1200".compareTo(x._2.depTime) <= 0);


		part1.join(part2)
		.mapToPair(x -> {
			return new Tuple2<Float, TomsFlight[]>(x._2._1.delay + x._2._2.delay, new TomsFlight[] {x._2._1, x._2._2});
		})


		.transformToPair(x -> x.sortByKey())

		.foreachRDD(rdd -> {
/*
			CassandraJavaUtil.javaFunctions(rdd).writerBuilder(
					"cloudcourse", 
					"g3q2", 
					CassandraJavaUtil.mapToRow(Person.class)).saveToCassandra();

	*/		
			
			try {
				Tuple2<Float, TomsFlight[]> flights = rdd.first();
				System.out.println("Flight 1: " + flights._2[0]);
				System.out.println("Flight 2: " + flights._2[1]);
				System.out.println("Total delay: " + flights._1);
			}
			catch(Exception e) {
				System.err.println("e: " + e);
			}
		});

		ctx.run();
		ctx.close();
	}

	public static void main(String[] args) throws InterruptedException {
		//new G3Q2Main("DFW", "ORD", "DFW", "2008-06-10", "2008-06-12");
		//new G3Q2Main("LAX", "ORD", "JFK", "2008-01-01", "2008-01-03");
		new G3Q2Main("CMI", "ORD", "LAX", "2008-03-04", "2008-03-06");
		//new G3Q2Main("JAX", "DFW", "CRP", "2008-09-09", "2008-09-11");
		//new G3Q2Main("SLC", "BFL", "LAX", "2008-04-01", "2008-04-03");
		//new G3Q2Main("LAX", "SFO", "PHX", "2008-07-12", "2008-07-14");
	}
}

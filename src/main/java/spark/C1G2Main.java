package spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.StringDecoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import cloudcourse.globals.DataSet;

import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;

public class C1G2Main {

	public static final void main(String[] args) throws InterruptedException {
		MyContext ctx = new MyContext();

		ctx.createStream()
		//.map(x -> x.value())
		.flatMap(x -> {
			String[] tokens =  x.value().substring(1, x.toString().length() - 1).split(",");
			
			List<Tuple2<String, Float>> list = new ArrayList<Tuple2<String, Float>>();
			if(tokens.length > DataSet.ARRDELAY && 
					tokens[DataSet.UNIQUECARRIER].isEmpty() == false && 
					tokens[DataSet.ARRDELAY].isEmpty() == false) {

				list.add(new Tuple2<String, Float>(tokens[DataSet.UNIQUECARRIER], Float.parseFloat(tokens[DataSet.ARRDELAY])));
			}
			return list.iterator();
		})
		
		//.countByValue()
		//.transform(x -> {
			//System.out.println("DAJO: " + x.sortByKey(false).take(10));
		//	return x.sortByKey(false);
		//})
		.print(100);

		/*
		JavaDStream<String> one = KafkaUtils.createDirectStream(
				ctx, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, params))

				// Read each line
				.map(x -> x.value())

				// Remove parenthesis, tokenize and return 5th field 
				.map(x -> x.toString().substring(1, x.toString().length() - 1).split(",")[DataSet.ORIGIN]);

		JavaPairDStream<String, Long> pair = one.mapToPair(x -> new Tuple2<String, Long>(x, 1L));

		JavaPairDStream<String, Long> ttt = pair.reduceByKey((x, y) -> (x+y));
		JavaPairDStream<String, Long> t = ttt.transformToPair(x -> x.sortByKey());

		t.print(1000);
		 */

		ctx.start();
		ctx.awaitTerminationOrTimeout(300000); //ctx.awaitTermination();
		ctx.stop(true, true); //ctx.stop();
		ctx.close();
	}
}

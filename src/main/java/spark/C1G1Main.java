package spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;

import cloudcourse.globals.DataSet;
import scala.Tuple2;

public class C1G1Main {
	private static final Map<String, Long> map = new HashMap<String, Long>();
	
	public static final void main(String[] args) throws InterruptedException {
		MyContext ctx = new MyContext();
		
		ctx.createStream()
		.map(x -> x.value())
		.flatMap(x -> {
			String[] tokens =  x.toString().substring(1, x.toString().length() - 1).split(",");
			List<String> list = new ArrayList<String>();

			if(tokens.length > DataSet.ORIGIN &&
					tokens[DataSet.ORIGIN].isEmpty() == false &&
					tokens[DataSet.DEST].isEmpty() == false) {

				list.add(tokens[DataSet.ORIGIN]);
				list.add(tokens[DataSet.DEST]);
			}
			return list.iterator();

		})
		.countByValue()
		//.mapToPair(x -> x.swap())
		.transformToPair(x -> {
			System.out.println("DAJO: " + x.sortByKey(false).take(10));
			return x.sortByKey(false);
		})
		.foreachRDD(x -> {
			x.foreach(y-> {
				if(map.containsKey(y._1)) {
					map.put(y._1, map.get(y._1) + y._2);
				} else {
					map.put(y._1, y._2);
				}
			});
			for(String s : map.keySet()) {
				System.out.println("x=" + s + ", y=" +  map.get(s));
			}
		});
		//.print(1000);

		ctx.start();
		ctx.awaitTerminationOrTimeout(1500000); //ctx.awaitTermination();
		ctx.stop(true, true); //ctx.stop();
		ctx.close();
	}
}

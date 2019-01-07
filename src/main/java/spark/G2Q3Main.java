package spark;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;
import cloudcourse.globals.DataSet;

public class G2Q3Main {
	private static final String[] FILTER = {"CMI_ORD", "IND_CMH", "DFW_IAH", "LAX_SFO", "JFK_LAX", "ATL_PHX"};

	public static final void main(String[] args) throws InterruptedException {
		MyContext ctx = new MyContext();

		ctx.createStream("cloudcourse")

		.flatMapToPair(x -> {
			String[] tokens =  x.value().substring(1, x.value().length() - 1).split(",");

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

		// Remember the keys 
		.updateStateByKey((nums, current) -> {

			Average average = current.or(new Average());
			for(float i : nums) {
				average.count++;
				average.sum += i;
			}
			return Optional.of(average);
		})

		.filter(x -> {
			for(String f : FILTER) {
				if(x._1.startsWith(f)) {
					return true;
				}
			}
			return false;
		})

		.transformToPair(x -> x.sortByKey(true))
	
		.print(1000);

		ctx.run();
		ctx.close();
	}
}

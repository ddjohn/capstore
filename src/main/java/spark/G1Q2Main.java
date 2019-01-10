package spark;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import cloudcourse.globals.DataSet;

public class G1Q2Main {

	public static final void main(String[] args) throws InterruptedException {
		MyContext ctx = new MyContext();

		ctx.createStream("cloudcourse")

		.flatMapToPair(x -> {
			
			// Parse input data
			String[] tokens =  x.value().substring(1, x.value().length() - 1).split(",");

			// Build list with (carrier, arrdelay)
			List<Tuple2<String, Float>> list = new ArrayList<Tuple2<String, Float>>();
			if(tokens.length > DataSet.ARRDELAY && 
					tokens[DataSet.UNIQUECARRIER].isEmpty() == false && 
					tokens[DataSet.ARRDELAY     ].isEmpty() == false) {

				list.add(new Tuple2<String, Float>(tokens[DataSet.UNIQUECARRIER], Float.parseFloat(tokens[DataSet.ARRDELAY])));
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
		
		// Print top 10
		.print();

		ctx.run();
		ctx.close();
	}
}

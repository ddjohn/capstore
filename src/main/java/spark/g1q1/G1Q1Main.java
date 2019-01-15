package spark.g1q1;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.Optional;
import mapreduce.globals.DataSet;
import scala.Tuple2;
import spark.globals.MyContext;

public class G1Q1Main {

	public static final void main(String[] args) throws InterruptedException {

		MyContext ctx = new MyContext();

		ctx.createStream("cloudcourse")

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

		// Remember the keys
		.updateStateByKey((nums, current) -> {
			long sum = current.or(0L); 
			for(long i : nums) {
				sum += i;
			}
			return Optional.of(sum);
		})

		// Sort by swapping values to keys and back		
		.mapToPair(x -> x.swap())
		.transformToPair(x -> x.sortByKey(false))
		.mapToPair(x -> x.swap())

		// Print top 10
		.print(10);

		ctx.run();
		ctx.close();
	}
}
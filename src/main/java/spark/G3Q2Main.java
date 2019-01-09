package spark;

import java.util.ArrayList;
import java.util.List;
import scala.Tuple2;
import cloudcourse.globals.DataSet;

public class G3Q2Main {

	public G3Q2Main(String origin, String middle, String dest, String originDate, String middleDate) throws InterruptedException {

		MyContext ctx = new MyContext();
		ctx.createStream("cloudcourse")
		.flatMapToPair(x -> {
			List<Tuple2<TomsFlight, Float>> list = new ArrayList<Tuple2<TomsFlight, Float>>();

			String[] tokens = x.value().substring(1, x.value().length() - 1).split(",");
			if(tokens.length > DataSet.DEPDELAY) {
				try {
					list.add(new Tuple2<TomsFlight, Float>(
							new TomsFlight(
									tokens[DataSet.ORIGIN], 
									tokens[DataSet.DEST], 
									null,
									tokens[DataSet.FLIGHTDATE],
									null),
							Float.parseFloat(tokens[DataSet.DEPDELAY])));
				}
				catch(Exception e) {
					System.out.println("e: " + e);
				}
			}
			return list.iterator();
		})
		.filter(x -> origin.compareTo(x._1.origin) == 0)
		.filter(x -> middle.compareTo(x._1.middle) == 0)
		.filter(x -> originDate.compareTo(x._1.originDate) == 0)
		.filter(x -> "1200".compareTo(x._1.originDate) < 0)

		.print();

		ctx.run();
		ctx.close();
	}

	public static void main(String[] args) throws InterruptedException {
		new G3Q2Main("DFW", "ORD", "DFW", "2008-06-10", "2008-06-12");
		//new G3Q2Main("LAX", "ORD", "JFK", "2008-01-01", "2008-01-03");
		//new G3Q2Main("CMI", "ORD", "LAX", "2008-03-04", "2008-03-06");
		//new G3Q2Main("JAX", "DFW", "CRP", "2008-09-09", "2008-09-11");
		//new G3Q2Main("SLC", "BFL", "LAX", "2008)-04-01", "2008-04-03");
		//new G3Q2Main("LAX", "SFO", "PHX", "2008-07-12", "2008-07-14");
	}
}

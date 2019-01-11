package spark.g2q1;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.Optional;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.ColumnSelector;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.datastax.spark.connector.writer.RowWriter;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.datastax.spark.connector.writer.SqlRowWriter;

import scala.Tuple2;
import scala.collection.IndexedSeq;
import scala.collection.generic.IndexedSeqFactory;
import scala.collection.immutable.Map;
import spark.Average;
import spark.MyContext;
import spark.g1q3.G1Q3Database;
import cloudcourse.globals.DataSet;

public class G2Q1Main {
	private static final String[] FILTER = {"CMI", "BWI", "MIA", "LAX", "IAH", "SFO"};

	public static final void main(String[] args) throws InterruptedException {
		MyContext ctx = new MyContext();

		ctx.createStream("cloudcourse")

		.flatMapToPair(x -> {

			// Parse input data
			String[] tokens =  x.value().substring(1, x.value().length() - 1).split(",");

			// Build list with (origin_carrier, depdelay)
			List<Tuple2<String, Float>> list = new ArrayList<Tuple2<String, Float>>();
			if(tokens.length > DataSet.DEPDELAY && 
					tokens[DataSet.ORIGIN       ].isEmpty() == false && 
					tokens[DataSet.UNIQUECARRIER].isEmpty() == false && 
					tokens[DataSet.DEPDELAY     ].isEmpty() == false) {

				list.add(new Tuple2<String, Float>(tokens[DataSet.ORIGIN] + "_" + tokens[DataSet.UNIQUECARRIER], Float.parseFloat(tokens[DataSet.DEPDELAY])));
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

		// Filter out fields of interest
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

		.map(x -> {
			String[] tokens = x._1.split("_");
			return new G2Q1Database(tokens[0], tokens[1], x._2.average());
		})

		.foreachRDD(rdd -> {

			CassandraJavaUtil.javaFunctions(rdd).writerBuilder(
					"cloudcourse", 
					"g2q1", 
					CassandraJavaUtil.mapToRow(G2Q1Database.class))
			.saveToCassandra();
		});

		// Print
		//.print(1000);

		ctx.run();
		ctx.close();
	}
}

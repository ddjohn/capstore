package spark;

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
import cloudcourse.globals.DataSet;

public class G2Q1Main {
	private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS c.g1q1 (PRIMARY KEY(origin, carrier), origin text, carrier text, delay double);";
	private static final String UPDATE_STMT = "UPDATE c.g1q1 SET delay = ? WHERE origin = ? AND carrier = ?;";
	
	public static final void main(String[] args) throws InterruptedException {

		//Cassandra cassandra = new Cassandra("c");
		//cassandra.query(CREATE_TABLE);
		//cassandra.close();

		MyContext ctx = new MyContext();
		ctx.createStream("cloudcourse")

		.flatMapToPair(x -> {
			String[] tokens =  x.value().substring(1, x.value().length() - 1).split(",");

			List<Tuple2<String, Float>> list = new ArrayList<Tuple2<String, Float>>();
			if(tokens.length > DataSet.DEPDELAY && 
					tokens[DataSet.ORIGIN].isEmpty() == false && 
					tokens[DataSet.UNIQUECARRIER].isEmpty() == false && 
					tokens[DataSet.DEPDELAY].isEmpty() == false) {
				
				list.add(new Tuple2<String, Float>(tokens[DataSet.ORIGIN] + "_" + tokens[DataSet.UNIQUECARRIER], Float.parseFloat(tokens[DataSet.DEPDELAY])));
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

		// Sort by swapping values to keys and back
		.mapToPair(x -> x.swap())
		.transformToPair(x -> x.sortByKey(true))
		.mapToPair(x -> x.swap())

		.foreachRDD(x -> {
			/*
			System.out.println(x);
			Object cassandraRdd = CassandraJavaUtil.javaFunctions(x)
					 .cassandraTable("c", "g1q1", CassandraJavaUtil.mapColumnTo(String.class),
							 CassandraJavaUtil.select("my_column"));
			
			CassandraJavaUtil.javaFunctions(x).saveToCassandra("c", "g1q1", 
					new RowWriterFactory<Tuple2<String, Average>>() {

						@Override
						public RowWriter<Tuple2<String, Average>> rowWriter(TableDef table, IndexedSeq<ColumnRef> arg1) {
							System.out.println("table: " + table);
							System.out.println("table: " + arg1);

							return null;
						}}, new ColumnSelector() {

							@Override
							public Map<String, String> aliases() {
								System.out.println("aliases");
								return null;
							}

							@Override
							public IndexedSeq<ColumnRef> selectFrom(TableDef table) {
								System.out.println("ttable:" + table);
								IndexedSeq<ColumnRef> a = new IndexedSeqFactory();
								return null;
							}});
			*/
			x.foreach(y -> {
				String[] cols = y._1.split("_");
				Cassandra c = new Cassandra("c");
				PreparedStatement s = c.prepareStatement(UPDATE_STMT);
				c.bind(s, Double.parseDouble(y._2.toString()), cols[0], cols[1]);		
				c.close();
			});
		});
		// Print top 10
		//.print(Integer.MAX_VALUE);

		ctx.run();
		ctx.close();
	}
}

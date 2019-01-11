package spark.globals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class MyContext extends JavaStreamingContext {

	static {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
	}
	
	private static final SparkConf conf = new SparkConf().setAppName("CloudCourse").setMaster("local");
	
	public MyContext() {
		super(conf, Durations.seconds(2));
		
		this.checkpoint("checkpoint");
	}

	public JavaInputDStream<ConsumerRecord<String, String>> createStream(String topic) {
		Set<String> topics = new HashSet<String>();
		topics.add(topic);

		Map<String, Object> params = new HashMap<String, Object>();
		params.put("bootstrap.servers", "localhost:9092");
		params.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");  
		params.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		params.put("group.id", "cloudcourse");

		return KafkaUtils.createDirectStream(this, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, params));
	}

	public void run() throws InterruptedException {
		this.start();
		this.awaitTerminationOrTimeout(15000000); //ctx.awaitTermination();
		this.stop(true, true); //ctx.stop();
	}
}

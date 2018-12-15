import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;


public class SparkSocketStreaming {

	public static void main(String[] args) {
	
		
		SparkConf sc = new SparkConf().setAppName("Spark Streaming Example").setMaster("local[*]");
		
		JavaStreamingContext sparkContexts = new JavaStreamingContext(sc,Durations.seconds(10));
		
		JavaReceiverInputDStream<String> Lines = sparkContexts.socketTextStream("localhost", Integer.parseInt("9999"), StorageLevel.MEMORY_AND_DISK());
		
		JavaDStream<String> result = Lines.flatMap(row -> 
			Arrays.asList(row.split(" ")).iterator()
		);
		

		
		
		JavaPairDStream<String, String> b = result.mapToPair(value -> new Tuple2<>(value,value))
				.reduceByKey((a, c) -> a+c);

		
			b.print();
	}

}

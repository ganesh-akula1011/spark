import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MapReduceDataSet {

	public static void main(String[] args) {

		List<String> list = Arrays.asList("ganesh","venkatesh","ajay","anand","gova","manoj","dhanaveer");
		
		
		SparkSession sc= new  SparkSession.Builder().master("local")
				.appName("Map-Reduce-Function").getOrCreate();
		
		Dataset<String> ds = sc.createDataset(list, Encoders.STRING()) ;
		
		Dataset<Row> df = ds.toDF();
	String result = ds.map((MapFunction<String, String>) row -> {
			return "Hello : "+row;
		},Encoders.STRING()).reduce((a,b)-> a+" "+ b);
		
		System.out.println(result);
	}

}

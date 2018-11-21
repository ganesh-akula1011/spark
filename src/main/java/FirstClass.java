import java.util.Properties;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class FirstClass{
	
	public static void main(String args[]) {
		
		SparkSession sc = new SparkSession.Builder()
				.appName("transform data to DB").master("local").getOrCreate();
		Dataset<Row> ds = sc.read().format("csv").option("header", true)
				.load("src/main/resources/name_and_comments.txt");
		ds.show();
		
		ds = ds.withColumn("full_name", concat(ds.col("first_name"),lit(" "),ds.col("last_name")));
	
//		ds = ds.filter(ds.col("comment").rlike("\\d+"));
		
		String dbConnectionUrl = "jdbc:postgresql://localhost/spark";
		Properties prop = new Properties();
	    prop.setProperty("driver", "org.postgresql.Driver");
	    prop.setProperty("user", "postgres");
	    prop.setProperty("password", "1234"); // <- The password you used while installing Postgres
	    ds.write().mode(SaveMode.Overwrite).jdbc(dbConnectionUrl, "firstclass", prop);
		ds.show();
	}
	
	
}
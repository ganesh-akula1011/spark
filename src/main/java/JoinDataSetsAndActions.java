import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
public class JoinDataSetsAndActions {

	public static void main(String[] args) {

		SparkSession sc = new SparkSession.Builder().appName("Join and Perform actions")
				.master("local").getOrCreate();
		
		Dataset<Row> customerDf = sc.read().format("csv").option("header", true)
				.option("inferSchema", "true")
		.load("src/main/resources/customers.csv");
		
		Dataset<Row> productDf = sc.read().format("csv").option("header", true)
				.option("inferSchema", "true")
		.load("src/main/resources/products.csv");
		
		Dataset<Row> purchaseDf = sc.read().format("csv").option("header", true)
				.option("inferSchema", "true")
		.load("src/main/resources/purchases.csv");
		
		
		Dataset<Row> joinedData = customerDf.join(purchaseDf, 
	    		customerDf.col("customer_id").equalTo(purchaseDf.col("customer_id")))
	    		.join(productDf, purchaseDf.col("product_id").equalTo(productDf.col("product_id")))
	    		.drop("favorite_website").drop(purchaseDf.col("customer_id"))
	    		.drop(purchaseDf.col("product_id")).drop("product_id");
		joinedData.groupBy("first_name","product_name")
		.agg(count(productDf.col("product_name").as("no. of purchases")), 
				sum(productDf.col("product_price").as("max_spent"))).drop("customer_id").show();
		
		
		
		
		
		
		
	}

}

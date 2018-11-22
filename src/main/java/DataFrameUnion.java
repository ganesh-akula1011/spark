import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class DataFrameUnion {

	public static void main(String[] args) {
		SparkSession sc = new SparkSession.Builder().master("local")
				.appName("Union the two data frames").getOrCreate();


		Dataset<Row> df1 = buildDf1(sc);
		Dataset<Row> df2 = buildDf2(sc);

		Dataset<Row> df = df1.unionByName(df2);
		//		String urlPath = "local"
		df.show(10);

	}

	private static Dataset<Row> buildDf2(SparkSession sc) {
		Dataset<Row> df = sc.read().format("csv")
				.option("header", true)
				.option("multiline", true).load("src/main/resources/philadelphia_recreations.csv");


		df = df.filter("lower(USE_) like '%park%' ");

		df = df.withColumn("park_id", concat(lit("phil_"), df.col("OBJECTID")))
				.withColumnRenamed("ASSET_NAME", "park_name")
				.withColumn("city", lit("Philadelphia"))
				.withColumnRenamed("ADDRESS", "address")
				.withColumn("has_playground", lit("UNKNOWN"))
				.withColumnRenamed("ZIPCODE", "zipcode")
				.withColumnRenamed("ACREAGE", "land_in_acres")
				.withColumn("geoX", lit("UNKNONW"))
				.withColumn("geoY", lit("UNKNONW"))
				.drop("SITE_NAME")
				.drop("OBJECTID")
				.drop("CHILD_OF")
				.drop("TYPE")
				.drop("USE_")
				.drop("DESCRIPTION")
				.drop("SQ_FEET")
				.drop("ALLIAS")
				.drop("CHRONOLOGY")
				.drop("NOTES")
				.drop("DATE_EDITED")
				.drop("EDITED_BY")
				.drop("OCCUPANT")
				.drop("TENANT")
				.drop("LABEL");
		return df;
	}

	private static Dataset<Row> buildDf1(SparkSession sc) {
		Dataset<Row> df = sc.read().format("json")
				.option("multiline", true)
				.option("header", true).load("src/main/resources/durham-parks.json");

		df = df.withColumn("park_id", concat(df.col("datasetid"), lit("_"), 
				df.col("fields.objectid"), lit("_Durham")))
				.withColumn("park_name", df.col("fields.park_name"))
				.withColumn("city", lit("Durham"))
				.withColumn("address", df.col("fields.address"))
				.withColumn("has_playground", df.col("fields.playground"))
				.withColumn("zipcode", df.col("fields.zip"))
				.withColumn("land_in_acres", df.col("fields.acres"))
				.withColumn("geoX", df.col("geometry.coordinates").getItem(0))
				.withColumn("geoY", df.col("geometry.coordinates").getItem(1))
				.drop("fields").drop("geometry").drop("record_timestamp").drop("recordid")
				.drop("datasetid");

		return df;
	}

}

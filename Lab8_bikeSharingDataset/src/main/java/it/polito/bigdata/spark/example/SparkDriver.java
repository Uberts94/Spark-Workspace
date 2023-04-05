package it.polito.bigdata.spark.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.count;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputFolder;

		inputPath = args[0];
		inputPath2 = args[1];
		threshold = Double.parseDouble(args[2]);
		outputFolder = args[3];

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().master("local").appName("Spark Lab #8 - bikeSharing").getOrCreate();

		// Invoke .master("local") to execute the application locally inside Eclipse
		// SparkSession ss = SparkSession.builder().master("local").appName("Spark Lab #8 - bikeSharing").getOrCreate();
		
		ss.udf().register("dayOfWeek", (Timestamp t) -> {
			return DateTool.DayOfTheWeek(t)+"-"+DateTool.hour(t);
		}, DataTypes.StringType);
		
		ss.udf().register("fullStation", (Integer i) -> {
			if(i == 0) return 1;
			else return 0;
		}, DataTypes.IntegerType);		
		
		ss.udf().register("counter", (Integer i) -> i, DataTypes.IntegerType);	
		
		Dataset<Row> timestamps = ss.read().format("csv").option("delimiter", "\t").option("header", true).option("inferSchema", true)
												.load(inputPath)
												.filter(row -> {
													if(row.getInt(row.fieldIndex("used_slots")) == 0
															&& row.getInt(row.fieldIndex("free_slots")) == 0) return false;
													else return true;
												}).selectExpr("station", "dayOfWeek(timestamp) as timestamp", "free_slots"
														, "fullStation(free_slots) as full", "counter(1) as counter")
												.groupBy("station", "timestamp")
												.agg(sum("full").as("full"), sum("counter").as("total"))
												.selectExpr("station", "timestamp", "full/total as criticality")
												.filter(row -> {
													if(row.getDouble(row.fieldIndex("criticality")) > threshold) return true;
													else return false;
												});
		
		Dataset<Row> stations = ss.read().format("csv").option("delimiter",  "\t").option("header", true).option("inferSchema", true)
										.load(inputPath2).select("id", "longitude", "latitude");
		
		timestamps.show();

		// Close the Spark session
		ss.stop();
	}
}

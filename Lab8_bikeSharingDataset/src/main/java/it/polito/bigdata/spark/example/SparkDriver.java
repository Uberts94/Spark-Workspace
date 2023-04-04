package it.polito.bigdata.spark.example;

import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String inputPath2;
//		Double threshold;
		String outputFolder;

		inputPath = args[0];
		inputPath2 = args[1];
//		threshold = Double.parseDouble(args[2]);
		outputFolder = args[2];

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().master("local").appName("Spark Lab #8 - bikeSharing").getOrCreate();

		// Invoke .master("local") to execute the application locally inside Eclipse
		// SparkSession ss = SparkSession.builder().master("local").appName("Spark Lab #8 - Template").getOrCreate();
		
		Dataset<StationTimestamp> timestamps = ss.read().format("csv").option("delimiter", "\t").option("header", true).option("inferSchema", true)
												.load(inputPath).as(Encoders.bean(StationTimestamp.class));
		
		Dataset<StationTimestamp> filtered = timestamps.filter(ts -> {
			System.out.println("station "+ ts.getStation());
			if(ts.getFree_slots() == 0 && ts.getUsed_slots() == 0) return false;
			else return true;
		});
		
//		Dataset<Station> stations = ss.read().format("csv").option("delimiter",  "\t").option("header", true).option("inferSchema", true).load(inputPath2)
//										.select("id", "longitude", "latitude").as(Encoders.bean(Station.class));
		

		
		timestamps.write().format("csv").option("header", true).save(outputFolder);

		// Close the Spark session
		ss.stop();
	}
}

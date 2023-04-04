package it.polito.bigdata.spark.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
//		String inputPath2;
//		Double threshold;
		String outputFolder;

		inputPath = args[0];
//		inputPath2 = args[1];
//		threshold = Double.parseDouble(args[2]);
		outputFolder = args[1];

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().master("local").appName("Spark Lab #8 - bikeSharing").getOrCreate();

		// Invoke .master("local") to execute tha application locally inside Eclipse
		// SparkSession ss = SparkSession.builder().master("local").appName("Spark Lab #8 - Template").getOrCreate();

		Dataset<Station> stations = ss.read().format("csv").option("delimiter",  "\t").option("header", true).option("inferSchema", true).load(inputPath)
										.as(Encoders.bean(Station.class));
		
		stations.write().format("csv").option("header", true).save(outputFolder);

		// Close the Spark session
		ss.stop();
	}
}

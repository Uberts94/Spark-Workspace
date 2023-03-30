package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.List;

import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
		String outputPath;
		
		inputPath=args[0];
		outputPath=args[1];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab #6").setMaster("local");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		//SparkConf conf=new SparkConf().setAppName("Spark Lab #6").setMaster("local");
		// Remember to remove .setMaster("local") before running your application on the cluster
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath);

		JavaPairRDD<String, String> userReviews= inputRDD.mapToPair(line -> {
			String[] entry = line.split(",");
			return new Tuple2<String, String>(entry[3], entry[2]);
		});
		
		// Store the result in the output folder
		inputRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}

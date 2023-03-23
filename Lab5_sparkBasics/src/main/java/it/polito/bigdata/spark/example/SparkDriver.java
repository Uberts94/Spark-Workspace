package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);


		String inputPath;
		String outputPath;
		String prefix;
		
		inputPath=args[0];
		outputPath=args[1];
		prefix=args[2];

		//SETUP FOR LOCAL APPLICATION RUN
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab5_sparkBasics").setMaster("local");
		
		//SETUP FOR CLUSTER APPLICATION RUN
		// Create a configuration object and set the name of the application
		//SparkConf conf=new SparkConf().setAppName("Spark Lab5_sparkBasics").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder
		// Each element/string of wordFreqRDD corresponds to one line of the input data 
		// (i.e, one pair "word\tfreq")  
		JavaRDD<String> wordFreqRDD = sc.textFile(inputPath);

		// Task 1: filtering input lines containing words that start with the specified prefix
		JavaRDD<String> filteredRDD = wordFreqRDD.filter(line -> {
			String[] entries = line.split("\t+");
			return entries[0].startsWith(prefix);
		});
		filteredRDD.saveAsTextFile(outputPath);
		
		/*
		 * Task 2
		 .......
		 .......
		 */

		// Close the Spark context
		sc.close();
	}
}

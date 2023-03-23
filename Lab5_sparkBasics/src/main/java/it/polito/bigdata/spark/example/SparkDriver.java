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
		String outputPath1;
		String outputPath2;
		String prefix;
		
		inputPath=args[0];
		outputPath1=args[1];
		outputPath2=args[2];
		prefix=args[3];

		//SETUP FOR LOCAL MAVEN_APPLICATION RUN
		// Create a configuration object and set the name of the application
		//SparkConf conf=new SparkConf().setAppName("Spark Lab5_sparkBasics").setMaster("local");
		
		//SETUP FOR CLUSTER APPLICATION RUN
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab5_sparkBasics");
		
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
		filteredRDD.saveAsTextFile(outputPath1);
		
		int maxFrequency = filteredRDD.map(line -> {
			String[] entries = line.split("\t+");
			return new Integer(Integer.parseInt(entries[1]));
		}).top(1).get(0);
		
		//printing on the stdout some statistics
		System.out.println("Maximum frequency: "+maxFrequency);
		System.out.println("Number of selected lines filter 1: "+filteredRDD.count());
		
		//Task 2: filtering filteredRDD to select lines containing words with a frequency 
		// greater than 80% of the maximum frequency
		JavaRDD<String> filtered2RDD = filteredRDD.filter(line -> {
			String[] entries = line.split("\t+");
			if(Double.parseDouble(entries[1]) > maxFrequency*0.8) return true;
			else return false;
		}).map(line -> {
			String[] entries = line.split("\t+");
			return entries[0];
		});
		filtered2RDD.saveAsTextFile(outputPath2);
		
		//printing on the stdout some statistics
		System.out.println("Number of selected lines filter 2: "+filtered2RDD.count());
		
		// Close the Spark context
		sc.close();
	}
}

package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
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

		JavaPairRDD<String, Iterable<String>> userReviews= inputRDD.filter(line -> !line.startsWith("Id")).mapToPair(line -> {
			String[] entry = line.split(",");
			return new Tuple2<String, String>(entry[2], entry[1]);
		}).distinct().groupByKey();
		
		JavaPairRDD<Tuple2<String, String>, Integer> pairsRDD = userReviews.filter(user -> {
			if(((Collection<String>) user._2()).size() > 1) return true;
			else return false;
		}).flatMapToPair(user -> {
			List<Tuple2<Tuple2<String, String>, Integer>> pairs = new ArrayList<Tuple2<Tuple2<String, String>, Integer>>();
			for(String p1 : user._2()) {
				for(String p2 : user._2()) {
					if(!p1.equals(p2)) {
						pairs.add(new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(p1, p2), 1));
					}
							
				}
			}
			return pairs.iterator();
		}).reduceByKey((pair1, pair2) -> pair1+pair2).filter(pair -> pair._2() > 1).mapToPair(pair -> {
			return new Tuple2<Integer, Tuple2<String, String>>(pair._2(),
					new Tuple2<String, String>(pair._1()._1(), pair._1()._2()));
		}).sortByKey(false).mapToPair(pair -> {
			return new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(pair._2()._1(), pair._2()._2()), pair._1());
		});

		// Store the result in the output folder
		pairsRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}

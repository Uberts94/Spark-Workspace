package it.polito.bigdata.spark.example;

import scala.Tuple2;

import org.apache.spark.api.java.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputFolder;

		inputPath = args[0];
		inputPath2 = args[1];
		threshold = Double.parseDouble(args[2]);
		outputFolder = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Lab #7").setMaster("local");

		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #7").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> registerRDD = sc.textFile(inputPath).filter(line -> !line.startsWith("station")).filter(line -> {
			String[] timestamp = line.split("\\t");
			if(Integer.parseInt(timestamp[2]) == 0 && Integer.parseInt(timestamp[3]) == 0) return false;
			else return true;
		});
		
		// <Tuple2<stationId, timeslot>, counterFull/total>
		JavaPairRDD<Tuple2<String, String>, Float> criticalStations = registerRDD.mapToPair(line -> {
			String[] entry = line.split("\\t");
			String[] date = entry[1].split(" ");
			String[] hour = date[1].split(":");
			int critical = 0;
			if(Integer.parseInt(entry[3]) == 0) critical++;
			return new Tuple2<Tuple2<String, String>, Tuple2<Integer, Integer>>(
						new Tuple2<String, String>(entry[0], DateTool.DayOfTheWeek(date[0])+"-"+hour[0]),
						new Tuple2<Integer, Integer>(critical, 1)
					);
		}).reduceByKey((staTime1, staTime2) -> {
			return new Tuple2<Integer, Integer>(staTime1._1()+staTime2._1(), staTime1._2()+staTime2._2());
		}).mapToPair(station -> {
			return new Tuple2<Tuple2<String, String>, Float>(station._1(), 
						(float) station._2()._1()/station._2()._2());
		}).filter(station -> station._2() > threshold);
		
		// <Tuple2<stationId, timeslot>, criticalValue>
		JavaPairRDD<String, Tuple2<String, Float>> criticalTimestamp = criticalStations.mapToPair(station -> {
			return new Tuple2<String, Tuple2<String, Float>>(
							station._1()._1(),
							new Tuple2<String, Float>(station._1()._2(), station._2())
					);
		}).groupByKey().mapToPair(station -> {
			String topTimestamp = "";
			float topCritical = Float.MIN_VALUE;
			for(Tuple2<String, Float> value : station._2()) {
				if(value._2() > topCritical) {
					topCritical = value._2();
					topTimestamp = new String(value._1());
				}
			}
			return new Tuple2<String, Tuple2<String, Float>>(
							station._1(),
							new Tuple2<String, Float>(topTimestamp, topCritical)
					);
		});
		
		JavaRDD<String> stationsRDD = sc.textFile(inputPath2);
		
		// Store in resultKML one String, representing a KML marker, for each station 
				// with a critical timeslot 
				// JavaRDD<String> resultKML = .....
		
		JavaRDD<String> resultKML = criticalTimestamp.join(stationsRDD.filter(line -> !line.startsWith("id")).mapToPair(line -> {
			String[] station = line.split("\\t");
			return new Tuple2<String, String>(station[0], station[1]+","+station[2]);
		})).map(station -> {
			String[] timestamp = station._2()._1()._1().split("-");
			String dayWeek = new String(timestamp[0]);
			String hour = new String(timestamp[1]);
			return "<Placemark><name>"+station._1()+"<ExtendedData><Data"
					+"name=\"DayWeek\"><value>"+dayWeek+"</value></Data><Data"
					+"name=\"Hour\"><value>"+hour+"</value></Data><Data"
					+"name=\"Criticality\"><value>"+station._2()._1()._2()+"</value></Data></ExtendedData><"
					+"Point><coordinates>"+station._2()._2()+"</coordinates></Point></Placemark>";
		});
		  
		// Invoke coalesce(1) to store all data inside one single partition/i.e., in one single output part file
		resultKML.coalesce(1).saveAsTextFile(outputFolder); 

		// Close the Spark context
		sc.close();
	}
}

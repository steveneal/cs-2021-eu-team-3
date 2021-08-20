package com.cs.rfq.decorator;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

public class RfqDecoratorMain {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");
        System.setProperty("spark.driver.allowMultipleContexts", "true");

        SparkSession session = SparkSession.builder()
                .appName("Gecko")
                .getOrCreate();

        JavaSparkContext spark = new JavaSparkContext(session.sparkContext());

        //SparkConf conf = new SparkConf().setAppName("Gecko");
        JavaStreamingContext streamingContext = new JavaStreamingContext(spark.getConf(), Durations.seconds(5));

        JavaDStream<String> lines = streamingContext.socketTextStream("localhost", 9000);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        //print out the results
        words.foreachRDD(rdd -> {
            rdd.collect().forEach(line -> System.out.println(line));
        });

        streamingContext.start();
        streamingContext.awaitTermination();

        spark.stop();

        //TODO: create a Spark configuration and set a sensible app name( DONE)

        //TODO: create a Spark streaming context

        //TODO: create a Spark session

        //TODO: create a new RfqProcessor and set it listening for incoming RFQs
    }

}

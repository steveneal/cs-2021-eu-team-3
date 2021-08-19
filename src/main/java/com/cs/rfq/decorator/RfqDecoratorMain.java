package com.cs.rfq.decorator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class RfqDecoratorMain {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");

        SparkSession session = SparkSession.builder()
                .appName("Scooter")
                .getOrCreate();

        JavaSparkContext spark = new JavaSparkContext(session.sparkContext());

        spark.stop();

        //TODO: create a Spark configuration and set a sensible app name( DONE)

        //TODO: create a Spark streaming context

        //TODO: create a Spark session

        //TODO: create a new RfqProcessor and set it listening for incoming RFQs
    }

}

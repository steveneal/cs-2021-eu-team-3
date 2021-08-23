package com.cs.rfq.decorator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

public class RfqDecoratorMain {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[*]");
        //System.setProperty("spark.driver.allowMultipleContexts", "true");

        SparkSession session = SparkSession.builder()
                .appName("Gecko")
                .getOrCreate();

        JavaSparkContext spark = new JavaSparkContext(session.sparkContext());
        JavaStreamingContext streamingContext = new JavaStreamingContext(spark, Durations.seconds(5));
        TradeDataLoader td = new TradeDataLoader();

        RfqProcessor processor = new RfqProcessor(session, streamingContext, td.getClass().getResource("loader-test-trades.json").getPath());

        processor.startSocketListener();

    }

}

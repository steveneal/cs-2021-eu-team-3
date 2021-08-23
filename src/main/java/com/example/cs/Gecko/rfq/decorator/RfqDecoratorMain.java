package com.example.cs.Gecko.rfq.decorator;

import com.example.cs.Gecko.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.example.cs.Gecko.rfq.decorator.publishers.MetadataRESTPublisher;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Map;

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

        RfqProcessor processor = new RfqProcessor(session, streamingContext, "C:\\exercises\\Experimental\\cs-2021-eu-team-3\\src\\main\\resources\\trades.json");

        processor.startSocketListener();

    }

}

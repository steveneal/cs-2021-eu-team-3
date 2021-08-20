package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.*;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.sum;

public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private final JavaStreamingContext streamingContext;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext, String path) {
        this.session = session;
        this.streamingContext = streamingContext;

        //TODO: use the TradeDataLoader to load the trade data archives
        this.trades = new TradeDataLoader().loadTrades(session, path);

        //TODO: take a close look at how these two extractors are implemented
        extractors.add(new TotalTradesWithEntityAndInstrumentExtractor());
        extractors.add(new VolumeTradedWithEntityYTDExtractor());
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new AverageTradedPriceExtractor());
    }

    public void startSocketListener() throws InterruptedException {

        JavaDStream<String> lines = streamingContext.socketTextStream("localhost", 9000);
        lines.foreachRDD(rdd -> {
            Rfq rfq = Rfq.fromJson(rdd.toString());
            processRfq(rfq);
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

     public Map<RfqMetadataFieldNames, Object> processRfqInternal(Rfq rfq) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));

        //create a blank map for the metadata to be collected
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();

        //TODO: get metadata from each of the extractors
        for (RfqMetadataExtractor data: extractors){
            metadata.putAll(data.extractMetaData(rfq, session, trades));
        }

        // return to main process function
        return metadata;
    }

    public void processRfq(Rfq rfq) {
        // publish
        Map<RfqMetadataFieldNames, Object> metadata = processRfqInternal(rfq);
        publisher.publishMetadata(metadata);
    }
}

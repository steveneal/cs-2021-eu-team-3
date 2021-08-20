package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.mean;

public class AverageTradedPriceExtractor implements RfqMetadataExtractor{


    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();



        double mean = 0;
        Dataset<Row> filtered = trades.filter(trades.col("SecurityId").equalTo(rfq.getIsin()));
        filtered = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastWeekMs)));

        if(filtered.count() > 0){
            Dataset<Row> meanRdd = filtered.select(mean(filtered.col("LastPx")));
            mean = meanRdd.first().getDouble(0);
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(averageTradedPriceMonthly, mean);

        return results;
    }



}

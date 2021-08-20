package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class TradeSideBiasExtractor implements RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        long pastMonthMs = DateTime.now().withMillis(todayMs).minusMonths(1).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()));

        Dataset<Row> filterWeek = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastWeekMs)));
        long counter = filterWeek.count();
        long buysWeek = filterWeek.filter(trades.col("Side").$eq$eq$eq(1)).count();
        long sellsWeek = filterWeek.filter(trades.col("Side").$eq$eq$eq(2)).count();
        buysWeek = buysWeek + sellsWeek == 0L ? -1 : buysWeek;
        sellsWeek = sellsWeek == 0L ? 1 : sellsWeek;
        double biasWeek = (double) buysWeek / (double) sellsWeek;
        Dataset<Row> filterMonth = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastMonthMs)));
        long buysMonth = filterWeek.filter(trades.col("Side").$eq$eq$eq(1)).count();
        long sellsMonth = filterWeek.filter(trades.col("Side").$eq$eq$eq(2)).count();
        buysMonth = buysMonth + sellsMonth == 0L ? -1L : buysMonth;
        sellsMonth = sellsMonth == 0L ? 1L : sellsMonth;
        double biasMonth = (double) buysMonth / (double) sellsMonth;

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(tradeSideBiasWeekly, biasWeek);
        results.put(tradeSideBiasMonthly, biasMonth);
        return results;
    }

}

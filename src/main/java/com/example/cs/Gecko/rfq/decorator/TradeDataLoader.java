package com.example.cs.Gecko.rfq.decorator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.types.DataTypes.*;

public class TradeDataLoader {

    private final static Logger log = LoggerFactory.getLogger(TradeDataLoader.class);

    public Dataset<Row> loadTrades(SparkSession session, String path) {
        //TODO: create an explicit schema for the trade data in the JSON files
        StructType schema =
                new StructType(new StructField[] {
                        new StructField("TraderId", LongType, false, Metadata.empty()),
                        new StructField("EntityId", LongType, false, Metadata.empty()),
                        new StructField("MsgType", IntegerType, false, Metadata.empty()),
                        new StructField("TradeReportId", LongType, false, Metadata.empty()),
                        new StructField("PreviouslyReported", StringType, false, Metadata.empty()),
                        new StructField("SecurityID", StringType, false, Metadata.empty()),
                        new StructField("SecurityIdSource", IntegerType, false, Metadata.empty()),
                        new StructField("LastQty", LongType, false, Metadata.empty()),
                        new StructField("LastPx", DoubleType, false, Metadata.empty()),
                        new StructField("TradeDate", DateType, false, Metadata.empty()),
                        new StructField("TransactTime", StringType, false, Metadata.empty()),
                        new StructField("NoSides", IntegerType, false, Metadata.empty()),
                        new StructField("Side", IntegerType, false, Metadata.empty()),
                        new StructField("OrderId", LongType, false, Metadata.empty()),
                        new StructField("Currency", StringType, false, Metadata.empty())
                });;

        //TODO: load the trades dataset
        Dataset<Row> trades = session.read().schema(schema).json(path);

        //TODO: log a message indicating number of records loaded and the schema used
        //log.info(String.format("%d trades found", trades.count()));


        return trades;
    }

}

package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TotalTradesWithEntityAndInstrumentExtractorTest extends AbstractSparkUnitTest{

    private Rfq rfq;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("volume-instrument-1.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    void willExtractEntityAndInstrumentMetaDataFromTradeDataToday() {
        TotalTradesWithEntityAndInstrumentExtractor extractor = new TotalTradesWithEntityAndInstrumentExtractor();
        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object tradeWithEntityT = meta.get(RfqMetadataFieldNames.tradesWithEntityToday);

        assertEquals(1L, tradeWithEntityT);
    }

    @Test
    void willExtractEntityAndInstrumentMetaDataFromTradeDataWeek() {
        TotalTradesWithEntityAndInstrumentExtractor extractor = new TotalTradesWithEntityAndInstrumentExtractor();
        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object tradeWithEntityT = meta.get(RfqMetadataFieldNames.tradesWithEntityPastWeek);

        assertEquals(2L, tradeWithEntityT);
    }

    @Test
    void willExtractEntityAndInstrumentMetaDataFromTradeDataYear() {
        TotalTradesWithEntityAndInstrumentExtractor extractor = new TotalTradesWithEntityAndInstrumentExtractor();
        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object tradeWithEntityT = meta.get(RfqMetadataFieldNames.tradesWithEntityPastYear);

        assertEquals(5L, tradeWithEntityT);
    }
}
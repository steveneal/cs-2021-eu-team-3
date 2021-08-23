package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TotalTradesWithInstrumentExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("volume-instrument-2.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    void willExtractEntityAndInstrumentMetaDataFromTradeDataToday() {
        TotalTradesWithInstrumentExtractor extractor = new TotalTradesWithInstrumentExtractor();
        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object tradeWithEntityT = meta.get(RfqMetadataFieldNames.tradesWithInstrumentToday);

        assertEquals(0L, tradeWithEntityT);
    }

    @Test
    void willExtractEntityAndInstrumentMetaDataFromTradeDataWeek() {
        TotalTradesWithInstrumentExtractor extractor = new TotalTradesWithInstrumentExtractor();
        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object tradeWithEntityT = meta.get(RfqMetadataFieldNames.tradesWithInstrumentPastWeek);

        assertEquals(2L, tradeWithEntityT);
    }

    @Test
    void willExtractEntityAndInstrumentMetaDataFromTradeDataYear() {
        TotalTradesWithInstrumentExtractor extractor = new TotalTradesWithInstrumentExtractor();
        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object tradeWithEntityT = meta.get(RfqMetadataFieldNames.tradesWithInstrumentPastYear);

        assertEquals(3L, tradeWithEntityT);
    }
}
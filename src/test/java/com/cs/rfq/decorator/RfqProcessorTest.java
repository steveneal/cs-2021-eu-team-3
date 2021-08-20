package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;
import static org.junit.jupiter.api.Assertions.*;

class RfqProcessorTest extends AbstractSparkUnitTest {

    RfqProcessor rfqProcessor;
    Map<RfqMetadataFieldNames, Object> actual;

    @BeforeEach
    public void setup() {
        String filePath = getClass().getResource("loader-test-trades-2021.json").getPath();
        rfqProcessor = new RfqProcessor(session, null, filePath);

        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000383864', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
        "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);
        actual = rfqProcessor.processRfqInternal(rfq);
    }

    @Test
    void testVolumeTradedYearToDate() { assertEquals(0L, actual.get(volumeTradedYearToDate)); }
    @Test
    void testTradesWithEntityAndInstrumentPastWeek() { assertEquals(0L, actual.get(tradesWithEntityAndInstrumentPastWeek)); }
    @Test
    void testTradesWithEntityAndInstrumentToday() { assertEquals(0L, actual.get(tradesWithEntityAndInstrumentToday)); }
    @Test
    void testTradesWithEntityToday() { assertEquals(0L, actual.get(tradesWithEntityToday)); }
    @Test
    void testTradesWithEntityPastWeek() { assertEquals(2L, actual.get(tradesWithEntityPastWeek)); }
    @Test
    void testTradesWithEntityPastYear() { assertEquals(5L, actual.get(tradesWithEntityPastYear)); }
    @Test
    void testTradesWithEntityAndInstrumentPastYear() { assertEquals(0L, actual.get(tradesWithEntityAndInstrumentPastYear)); }

}
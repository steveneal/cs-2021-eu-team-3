package com.cs.rfq.decorator.publishers;

import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.util.Map;

public class MetadataJSONFilePublisher implements MetadataPublisher {

    private static final Logger log = LoggerFactory.getLogger(MetadataJsonLogPublisher.class);

    @Override
    public void publishMetadata(Map<RfqMetadataFieldNames, Object> metadata) {
        String s = new GsonBuilder().setPrettyPrinting().create().toJson(metadata);
        try{
            FileWriter writer = new FileWriter("C:\\exercises\\cs-2021-eu-team-3\\src\\main\\resources\\rfqJSONFormat.json");
            writer.write(s);
            writer.close();
        } catch(Exception ex){
            log.info(ex.getMessage());
        }

        log.info(String.format("Publishing metadata:%n%s", s));
    }
}

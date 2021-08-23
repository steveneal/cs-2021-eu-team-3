package com.example.cs.Gecko.rfq.decorator.publishers;

import com.example.cs.Gecko.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MetadataRESTPublisher implements MetadataPublisher {

    private static final Logger log = LoggerFactory.getLogger(MetadataJsonLogPublisher.class);

    private String latestMetaData;
    private Boolean isReady = false;

    @Override
    public void publishMetadata(Map<RfqMetadataFieldNames, Object> metadata) {
        latestMetaData = new GsonBuilder().setPrettyPrinting().create().toJson(metadata);
        isReady = true;
    }

    public Boolean isMetaDataReady(){
        return isReady;
    }

    public String fetchLatestMetaData(){
        if(isReady){
            isReady = false;
            return latestMetaData;
        }
        else{
            return "Metadata not ready yet";
        }
    }
}

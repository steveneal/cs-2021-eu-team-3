package com.example.cs.Gecko.rfq.decorator.publishers;

import com.example.cs.Gecko.rfq.decorator.extractors.RfqMetadataFieldNames;

import java.util.Map;

/**
 * Simple interface for different metadata publishers to implement
 */
public interface MetadataPublisher {
    void publishMetadata(Map<RfqMetadataFieldNames, Object> metadata);
}

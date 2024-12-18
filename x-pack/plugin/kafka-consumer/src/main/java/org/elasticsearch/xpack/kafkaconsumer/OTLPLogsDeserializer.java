/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kafkaconsumer;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OTLPLogsDeserializer implements Deserializer<ExportLogsServiceRequest> {
    private static final Logger logger = LogManager.getLogger(OTLPLogsDeserializer.class);

    @Override
    public ExportLogsServiceRequest deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            ExportLogsServiceRequest req = ExportLogsServiceRequest.parseFrom(data);
            return req;
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(String.format("failed to parse ExportLogsServiceRequest record from topic '%s'", topic), e);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kafkaconsumer;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class OTLPLogsDeserializer implements Deserializer<String> {
    private static Charset encoding = StandardCharsets.UTF_8;

    // TODO(axw)

    @Override
    public String deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        return new String(data, encoding);
    }

    @Override
    public String deserialize(String topic, Headers headers, ByteBuffer data) {
        if (data == null) {
            return null;
        }
        if (data.hasArray()) {
            return new String(data.array(), data.position() + data.arrayOffset(), data.remaining(), encoding);
        }
        return new String(Utils.toArray(data), encoding);
    }
}

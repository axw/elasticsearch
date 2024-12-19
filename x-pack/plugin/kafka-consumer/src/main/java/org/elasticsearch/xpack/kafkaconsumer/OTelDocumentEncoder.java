/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kafkaconsumer;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.resource.v1.Resource;

import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

public class OTelDocumentEncoder {
    public static XContentBuilder toXContent(XContentBuilder builder, LogRecord record) throws IOException {
        builder.timestampField("@timestamp", Instant.ofEpochSecond(0, record.getTimeUnixNano()));
        builder.timestampField("observed_timestamp", Instant.ofEpochSecond(0, record.getObservedTimeUnixNano()));
        builder.field("trace_id", record.getTraceId());
        builder.field("span_id", record.getSpanId());
        builder.field("severity_text", record.getSeverityText());
        builder.field("severity_number", record.getSeverityNumberValue());

        final AnyValue body = record.getBody();
        switch (body.getValueCase()) {
            // TODO body.structured/body.flattened, taking event.name into account
            case STRING_VALUE -> builder.field("body.text", body.getStringValue());
            default -> builder.field("body.text", body.toString());
        }

        builder.field("dropped_attributes_count", record.getDroppedAttributesCount());
        return attributesToXContent(builder, record.getAttributesList());
    }

    public static XContentBuilder toXContent(XContentBuilder builder, Resource resource, String schemaURL) throws IOException {
        builder.startObject("resource");
        if (schemaURL != null && !schemaURL.equals("")) {
            builder.field("schema_url", schemaURL);
        }
        builder.field("dropped_attributes_count", resource.getDroppedAttributesCount());
        attributesToXContent(builder, resource.getAttributesList());
        return builder.endObject();
    }

    public static XContentBuilder toXContent(XContentBuilder builder, InstrumentationScope scope, String schemaURL) throws IOException {
        builder.startObject("scope");
        final String name = scope.getName();
        final String version = scope.getVersion();
        if (!name.isEmpty()) {
            builder.field("name", name);
        }
        if (!version.isEmpty()) {
            builder.field("version", version);
        }
        if (schemaURL != null && !schemaURL.isEmpty()) {
            builder.field("schema_url", schemaURL);
        }
        builder.field("dropped_attributes_count", scope.getDroppedAttributesCount());
        attributesToXContent(builder, scope.getAttributesList());
        return builder.endObject();
    }

    public static XContentBuilder attributesToXContent(XContentBuilder builder, List<KeyValue> attributes) throws IOException {
        builder.startObject("attributes");
        for (KeyValue kv : attributes) {
            final String k = kv.getKey();
            final AnyValue v = kv.getValue();
            switch (v.getValueCase()) {
                // TODO other cases
                case INT_VALUE -> builder.field(k, v.getIntValue());
                case BOOL_VALUE -> builder.field(k, v.getBoolValue());
                case DOUBLE_VALUE -> builder.field(k, v.getDoubleValue());
                case STRING_VALUE -> builder.field(k, v.getStringValue());
            }
        }
        return builder.endObject();
    }
}

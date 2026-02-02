/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;

import com.google.protobuf.ByteString;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Builds one Elasticsearch document per OTLP Span, compatible with the
 * elasticsearchexporter otel mapping mode and the traces-otel index template.
 */
public class TraceDocumentBuilder {

    private static final String SPAN_KIND_PREFIX = "SPAN_KIND_";
    private static final String STATUS_CODE_PREFIX = "STATUS_CODE_";

    private final BufferedByteStringAccessor byteStringAccessor;

    public TraceDocumentBuilder(BufferedByteStringAccessor byteStringAccessor) {
        this.byteStringAccessor = byteStringAccessor;
    }

    /**
     * Build a span document into the given builder and return the target index name.
     */
    public String buildSpanDocument(
        XContentBuilder builder,
        Resource resource,
        InstrumentationScope scope,
        Span span,
        TargetIndex targetIndex
    ) throws IOException {
        builder.startObject();
        long startNanos = span.getStartTimeUnixNano();
        builder.field("@timestamp", TimeUnit.NANOSECONDS.toMillis(startNanos));
        buildDataStream(builder, targetIndex);
        buildResource(builder, resource);
        // Span identity and core fields
        addHexIdIfNonEmpty(builder, "trace_id", span.getTraceId());
        addHexIdIfNonEmpty(builder, "span_id", span.getSpanId());
        addHexIdIfNonEmpty(builder, "parent_span_id", span.getParentSpanId());
        if (span.getTraceState().isEmpty() == false) {
            builder.field("trace_state", span.getTraceState());
        }
        if (span.getName().isEmpty() == false) {
            builder.field("name", span.getName());
        }
        builder.field("kind", spanKindToString(span.getKind()));
        long endNanos = span.getEndTimeUnixNano();
        if (endNanos >= startNanos) {
            builder.field("duration", endNanos - startNanos);
        }
        // Attributes (exclude target-index attributes per otel mapping mode)
        builder.startObject("attributes");
        buildAttributes(builder, span.getAttributesList());
        builder.endObject();
        if (span.getDroppedAttributesCount() > 0) {
            builder.field("dropped_attributes_count", span.getDroppedAttributesCount());
        }
        // Links
        List<Span.Link> links = span.getLinksList();
        if (links.isEmpty() == false) {
            builder.startArray("links");
            for (int i = 0; i < links.size(); i++) {
                buildLink(builder, links.get(i));
            }
            builder.endArray();
        }
        if (span.getDroppedLinksCount() > 0) {
            builder.field("dropped_links_count", span.getDroppedLinksCount());
        }
        if (span.getDroppedEventsCount() > 0) {
            builder.field("dropped_events_count", span.getDroppedEventsCount());
        }
        // Status
        if (span.hasStatus()) {
            buildStatus(builder, span.getStatus());
        }
        builder.endObject();
        return targetIndex.index();
    }

    private void buildDataStream(XContentBuilder builder, TargetIndex targetIndex) throws IOException {
        if (targetIndex.isDataStream() == false) {
            return;
        }
        builder.startObject("data_stream");
        builder.field("type", targetIndex.type());
        builder.field("dataset", targetIndex.dataset());
        builder.field("namespace", targetIndex.namespace());
        builder.endObject();
    }

    private void buildResource(XContentBuilder builder, Resource resource) throws IOException {
        builder.startObject("resource");
        if (resource.getDroppedAttributesCount() > 0) {
            builder.field("dropped_attributes_count", resource.getDroppedAttributesCount());
        }
        builder.startObject("attributes");
        buildAttributes(builder, resource.getAttributesList());
        builder.endObject();
        builder.endObject();
    }

    private void buildAttributes(XContentBuilder builder, List<KeyValue> attributes) throws IOException {
        for (int i = 0, size = attributes.size(); i < size; i++) {
            KeyValue attribute = attributes.get(i);
            String key = attribute.getKey();
            if (TargetIndex.isTargetIndexAttribute(key) == false) {
                builder.field(key);
                attributeValue(builder, attribute.getValue());
            }
        }
    }

    private void attributeValue(XContentBuilder builder, AnyValue value) throws IOException {
        switch (value.getValueCase()) {
            case STRING_VALUE -> byteStringAccessor.utf8Value(builder, value.getStringValueBytes());
            case BOOL_VALUE -> builder.value(value.getBoolValue());
            case INT_VALUE -> builder.value(value.getIntValue());
            case DOUBLE_VALUE -> builder.value(value.getDoubleValue());
            case ARRAY_VALUE -> {
                builder.startArray();
                List<AnyValue> valuesList = value.getArrayValue().getValuesList();
                for (int i = 0, valuesListSize = valuesList.size(); i < valuesListSize; i++) {
                    attributeValue(builder, valuesList.get(i));
                }
                builder.endArray();
            }
            default -> throw new IllegalArgumentException("Unsupported attribute value type: " + value.getValueCase());
        }
    }

    private void addHexIdIfNonEmpty(XContentBuilder builder, String fieldName, ByteString id) throws IOException {
        if (id != null && id.isEmpty() == false) {
            builder.field(fieldName, bytesToHex(id));
        }
    }

    private static String bytesToHex(ByteString bytes) {
        int n = bytes.size();
        StringBuilder sb = new StringBuilder(n * 2);
        for (int i = 0; i < n; i++) {
            sb.append(String.format("%02x", bytes.byteAt(i) & 0xff));
        }
        return sb.toString();
    }

    private static String spanKindToString(Span.SpanKind kind) {
        String name = kind.name();
        return name.startsWith(SPAN_KIND_PREFIX) ? name.substring(SPAN_KIND_PREFIX.length()) : name;
    }

    private void buildLink(XContentBuilder builder, Span.Link link) throws IOException {
        builder.startObject();
        addHexIdIfNonEmpty(builder, "trace_id", link.getTraceId());
        addHexIdIfNonEmpty(builder, "span_id", link.getSpanId());
        if (link.getTraceState().isEmpty() == false) {
            builder.field("trace_state", link.getTraceState());
        }
        if (link.getAttributesCount() > 0) {
            builder.startObject("attributes");
            buildAttributes(builder, link.getAttributesList());
            builder.endObject();
        }
        if (link.getDroppedAttributesCount() > 0) {
            builder.field("dropped_attributes_count", link.getDroppedAttributesCount());
        }
        builder.endObject();
    }

    private void buildStatus(XContentBuilder builder, Status status) throws IOException {
        builder.startObject("status");
        if (status.getMessage().isEmpty() == false) {
            builder.field("message", status.getMessage());
        }
        builder.field("code", statusCodeToString(status.getCode()));
        builder.endObject();
    }

    /**
     * Map OTLP StatusCode to the string used by elasticsearchexporter otel mode (e.g. "2xx" for OK).
     */
    private static String statusCodeToString(Status.StatusCode code) {
        return switch (code) {
            case STATUS_CODE_OK -> "2xx";
            case STATUS_CODE_ERROR -> "ERROR";
            default -> "UNSET";
        };
    }
}

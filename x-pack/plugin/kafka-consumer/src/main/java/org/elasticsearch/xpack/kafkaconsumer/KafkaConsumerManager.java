/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kafkaconsumer;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.resource.v1.Resource;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class KafkaConsumerManager extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(KafkaConsumerManager.class);

    private final Properties clientConfig;
    private final ClusterService clusterService;
    private final ExecutorService executorService;
    private final Client client;

    public KafkaConsumerManager(Properties clientConfig, ClusterService clusterService, ExecutorService executorService, Client client) {
        this.clientConfig = clientConfig;
        this.clusterService = clusterService;
        this.executorService = executorService;
        this.client = client;
    }

    @Override
    protected void doStart() {
        // NOTE(axw) KafkaConsumerManager should be watching for some stream
        // definitions that tell it to start/stop Kafka consumers, and their
        // configuration: Kafka client, topics, codec(s), consumer group ID,
        // and destination data stream(s).
        //
        // We're hardcoding for expediency.
        Properties consumerConfig = new Properties();
        consumerConfig.putAll(clientConfig);
        consumerConfig.put("group.id", "elasticsearch");
        try {
            consumerConfig.put("key.deserializer", Class.forName("org.apache.kafka.common.serialization.StringDeserializer"));
            consumerConfig.put("value.deserializer", OTLPLogsDeserializer.class);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        addConsumer(
            consumerConfig,
            // TODO(axw) how should we configure topic patterns and expected schema?
            //
            // OTel Collector exports to otlp_logs|otlp_spans|otlp_metrics by default.
            // Ideally we would also support multiple schemas in one topic, with different
            // schema registry strategies.
            //
            // Could we have another plugin (e.g. otel-data) register a schema (XContentRegistry?),
            // and reference it by a Content-Type record header?
            Pattern.compile("otlp_logs")
        );
    }

    private void addConsumer(Properties consumerConfig, Pattern topicPattern) {
        executorService.submit(new KafkaConsumerRunnable(client, consumerConfig, topicPattern));
    }

    @Override
    protected void doStop() {
        for (Runnable runnable : executorService.shutdownNow()) {
            KafkaConsumerRunnable k = (KafkaConsumerRunnable) runnable;
            try {
                k.shutdown();
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    @Override
    protected void doClose() throws IOException {
        executorService.close();
    }

    private static class KafkaConsumerRunnable implements Runnable {
        private static final Logger logger = LogManager.getLogger(KafkaConsumerRunnable.class);

        private final Client client;
        private final Properties consumerConfig;
        private final Pattern topicPattern;
        private final CountDownLatch shutdownLatch;
        private boolean isShutdown;
        private KafkaConsumer<String, ExportLogsServiceRequest> consumer;

        KafkaConsumerRunnable(Client client, Properties consumerConfig, Pattern topicPattern) {
            this.client = client;
            this.consumerConfig = consumerConfig;
            this.topicPattern = topicPattern;
            this.shutdownLatch = new CountDownLatch(1);
            this.isShutdown = false;
            this.consumer = null;
        }

        void shutdown() throws InterruptedException {
            synchronized (this) {
                isShutdown = true;
                if (consumer != null) {
                    consumer.wakeup();
                }
            }
            shutdownLatch.await();
        }

        public void run() {
            boolean shuttingDown = false;
            final KafkaConsumer<String, ExportLogsServiceRequest> consumer = new KafkaConsumer<>(consumerConfig);
            try {
                consumer.subscribe(topicPattern);
                synchronized (this) {
                    if (!isShutdown) {
                        this.consumer = consumer;
                    }
                }
                while (!shuttingDown) {
                    ConsumerRecords<String, ExportLogsServiceRequest> records = consumer.poll(Duration.ofSeconds(10));
                    if (records.isEmpty()) {
                        continue;
                    }

                    // TODO(axw) we should consider creating a bulk request for
                    // all of the consumed records, rather than one per record.

                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    for (ConsumerRecord<String, ExportLogsServiceRequest> record : records) {
                        logger.debug("process logs record", record);
                        if (processLogs(record)) {
                            final TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                            final OffsetAndMetadata prev = offsets.get(tp);
                            final OffsetAndMetadata curr = new OffsetAndMetadata(record.offset(), record.leaderEpoch(), null);
                            if (prev == null || prev.offset() == record.offset() - 1) {
                                offsets.put(tp, curr);
                            } else {
                                logger.warn(String.format("gap in offsets for %s: %s, %d", tp, prev, curr));
                            }
                        }
                    }
                    if (!offsets.isEmpty()) {
                        try {
                            consumer.commitSync(offsets); // commit after: at-least-once delivery
                        } catch (WakeupException e) {
                            shuttingDown = true;
                            consumer.commitSync();
                        }
                    }
                }
            } catch (WakeupException e) {
                // Signal to shut down.
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
                shutdownLatch.countDown();
            }
        }

        // processLogs returns true if the log record was successfully processed, and false otherwise.
        private boolean processLogs(ConsumerRecord<String, ExportLogsServiceRequest> record) {
            // Each document's ID is prefixed by the Kafka topic, partition, and record offset.
            // The record is a batch, so each log document we index additionally has its position
            // within the record encoded in the ID.
            final String idPrefix = String.format("%s#%d#%d#", record.topic(), record.partition(), record.offset());
            final String dataStream = "logs-generic.otel-default";
            final BulkRequestBuilder bulk = client.prepareBulk(dataStream);

            int documentOffset = 0;
            for (ResourceLogs resourceLogs : record.value().getResourceLogsList()) {
                final Resource resource = resourceLogs.getResource();
                final String resourceSchemaURL = resourceLogs.getSchemaUrl();
                for (ScopeLogs scopeLogs : resourceLogs.getScopeLogsList()) {
                    final InstrumentationScope scope = scopeLogs.getScope();
                    final String scopeSchemaURL = scopeLogs.getSchemaUrl();
                    for (LogRecord logRecord : scopeLogs.getLogRecordsList()) {
                        try (XContentBuilder builder = jsonBuilder()) {
                            IndexRequestBuilder item = client.prepareIndex();
                            item.setCreate(true);
                            item.setId(idPrefix + (documentOffset++));
                            builder.startObject();
                            toXContent(builder, resource, resourceSchemaURL);
                            toXContent(builder, scope, scopeSchemaURL);
                            toXContent(builder, logRecord);
                            item.setSource(builder.endObject());
                            bulk.add(item);
                        } catch (Exception e) {
                            // TODO(axw) can this ever even happen? If so, then we should
                            // consider storing the original document in some degraded form.
                            logger.error(e);
                        }
                    }
                }
            }
            try {
                // TODO(axw) should the bulk request be executed as the user
                // who configured the consumer, like Transforms?
                BulkResponse response = bulk.execute().get();
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("bulk indexing request for %d item(s) took %s", documentOffset, response.getTook()));
                }

                boolean allSuccess = true;
                if (response.hasFailures()) {
                    for (BulkItemResponse item : response) {
                        final BulkItemResponse.Failure failure = item.getFailure();
                        if (failure == null) {
                            if (logger.isTraceEnabled()) {
                                logger.trace(String.format("successfully indexed %s in %s", item.getId(), item.getIndex()));
                            }
                            continue;
                        }
                        if (failure.getStatus() == RestStatus.CONFLICT) {
                            if (logger.isTraceEnabled()) {
                                logger.trace(String.format("ignoring conflict for %s in %s", item.getId(), item.getIndex()));
                            }
                            continue;
                        }
                        // TODO check failure store status
                        logger.error("failed to index item: " + item.getFailureMessage());
                        allSuccess = false;
                    }
                }
                return allSuccess;
            } catch (Exception e) {
                logger.error(e);
                return false;
            }
        }

        public XContentBuilder toXContent(XContentBuilder builder, LogRecord record) throws IOException {
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

        public XContentBuilder toXContent(XContentBuilder builder, Resource resource, String schemaURL) throws IOException {
            builder.startObject("resource");
            if (schemaURL != null && !schemaURL.equals("")) {
                builder.field("schema_url", schemaURL);
            }
            builder.field("dropped_attributes_count", resource.getDroppedAttributesCount());
            attributesToXContent(builder, resource.getAttributesList());
            return builder.endObject();
        }

        public XContentBuilder toXContent(XContentBuilder builder, InstrumentationScope scope, String schemaURL) throws IOException {
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

        public XContentBuilder attributesToXContent(XContentBuilder builder, List<KeyValue> attributes) throws IOException {
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
}

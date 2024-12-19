/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kafkaconsumer;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.resource.v1.Resource;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.action.index.IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class KafkaEngine extends InternalEngine implements Runnable {
    private static final Logger logger = LogManager.getLogger(KafkaEngine.class);

    public static final Setting<List<String>> KAFKA_BOOTSTRAP_SERVERS = Setting.stringListSetting(
        "kafka.bootstrap.servers",
        List.of("localhost:9092"),
        Setting.Property.NodeScope
    );

    // TODO the validator should prevent users from setting index.number_of_shards and other
    // settings that control sharding. Shards are expected to be controlled by this plugin,
    // so they can be kept synchronised with the Kafka partitions.
    public static final Setting<List<String>> INDEX_KAFKA_TOPIC = Setting.stringListSetting("index.kafka.topic", values -> {
        assert values != null : "Invalid null value for [index.kafka.topic].";
        for (String v : values) {
            Pattern.compile(v);
        }
    }, Setting.Property.IndexScope);

    public static final Setting<String> INDEX_KAFKA_KEY_DESERIALIZER = Setting.simpleString(
        "index.kafka.key.deserializer",
        StringDeserializer.class.getName(),
        name -> {
            try {
                Class.forName(name);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(e);
            }
        },
        Setting.Property.IndexScope
    );

    public static final Setting<String> INDEX_KAFKA_VALUE_DESERIALIZER = Setting.simpleString(
        "index.kafka.value.deserializer",
        StringDeserializer.class.getName(),
        name -> {
            try {
                Class.forName(name);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(e);
            }
        },
        Setting.Property.IndexScope
    );

    private final Client client;
    private final CountDownLatch shutdownLatch;
    private boolean isShutdown;
    private KafkaConsumer<String, ExportLogsServiceRequest> consumer;

    public KafkaEngine(EngineConfig config, Client client) {
        super(config);
        this.client = client;
        shutdownLatch = new CountDownLatch(1);
        isShutdown = false;
    }

    @Override
    public IndexResult index(Index index) {
        throw new UnsupportedOperationException("direct operations are not supported on a Kafka engine");
    }

    @Override
    public DeleteResult delete(Delete delete) {
        throw new UnsupportedOperationException("direct operations are not supported on a Kafka engine");
    }

    @Override
    public NoOpResult noOp(NoOp noOp) {
        throw new UnsupportedOperationException("direct operations are not supported on a Kafka engine");
    }

    public void start() {
        getEngineConfig().getThreadPool().executor(KafkaConsumerPlugin.THREAD_POOL_NAME).submit(this);
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            isShutdown = true;
            if (consumer != null) {
                consumer.wakeup();
            }
        }
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            logger.error(e);
        }
        super.close();
    }

    @Override
    public void run() {
        try {
            final Properties consumerConfig = new Properties();
            final List<String> bootstrapServers = KAFKA_BOOTSTRAP_SERVERS.get(getEngineConfig().getIndexSettings().getNodeSettings());
            final String keyDeserializer = getEngineConfig().getIndexSettings().getValue(INDEX_KAFKA_KEY_DESERIALIZER);
            final String valueDeserializer = getEngineConfig().getIndexSettings().getValue(INDEX_KAFKA_VALUE_DESERIALIZER);

            // Broker-level configuration.
            //
            // TODO supports TLS & auth configuration.
            // TODO should we support multiple brokers?
            consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.stream().collect(Collectors.joining(",")));
            consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, getEngineConfig().getIndexSettings().getNodeName());

            // Name the consumer group after the index. This enables multiple indices to consume from the topics.
            consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "elasticsearch:" + getEngineConfig().getShardId().getIndexName());
            consumerConfig.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "" + getEngineConfig().getShardId().getId());

            // Use a custom Elasticsearch-specific partition assignor that ensures partitions
            // are consumed by the node that owns the primary shard.
            consumerConfig.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, List.of(ShardPartitionAssignor.class));

            try {
                consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Class.forName(keyDeserializer));
                consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Class.forName(valueDeserializer));
            } catch (ClassNotFoundException e) { /* Checked by setting validators */ }

            // A list of topic patterns must be defined in index settings.
            final List<Pattern> topicPatterns = getEngineConfig().getIndexSettings()
                .getValue(INDEX_KAFKA_TOPIC)
                .stream()
                .map(Pattern::compile)
                .toList();

            // TODO implement a custom ConsumerPartitionAssignor.
            //
            // We should assign each partition to the node that holds the corresponding primary shard.
            // https://kafka.apache.org/24/javadoc/org/apache/kafka/clients/consumer/ConsumerPartitionAssignor.html
            logger.info("creating Kafka consumer for " + getEngineConfig().getShardId());
            final KafkaConsumer<String, ExportLogsServiceRequest> consumer = new KafkaConsumer<>(consumerConfig);
            synchronized (this) {
                if (isShutdown) {
                    return;
                }
                this.consumer = consumer;
            }
            for (Pattern pattern : topicPatterns) {
                consumer.subscribe(pattern);
                logger.info(String.format("consuming from %s into %s", pattern, getEngineConfig().getShardId()));
            }

            while (true) {
                ConsumerRecords<String, ExportLogsServiceRequest> records = consumer.poll(Duration.ofSeconds(10));
                if (records.isEmpty()) {
                    continue;
                }
                if (!processRecords(records)) {
                    return;
                }
            }
        } catch (WakeupException e) {
            // Signal to shut down.
        } catch (Exception e) {
            logger.error(e);
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            shutdownLatch.countDown();
            logger.info("closed Kafka consumer");
        }
    }

    private boolean processRecords(ConsumerRecords<String, ExportLogsServiceRequest> records) throws IOException {
        for (ConsumerRecord<String, ExportLogsServiceRequest> record : records) {
            // Each document's ID is prefixed by the Kafka topic, partition, and record offset.
            // The record is a batch, so each log document we index additionally has its position
            // within the record encoded in the ID.
            final String idPrefix = String.format("%s#%d#%d#", record.topic(), record.partition(), record.offset());

            int documentOffset = 0;
            for (ResourceLogs resourceLogs : record.value().getResourceLogsList()) {
                final Resource resource = resourceLogs.getResource();
                final String resourceSchemaURL = resourceLogs.getSchemaUrl();
                for (ScopeLogs scopeLogs : resourceLogs.getScopeLogsList()) {
                    final InstrumentationScope scope = scopeLogs.getScope();
                    final String scopeSchemaURL = scopeLogs.getSchemaUrl();
                    for (LogRecord logRecord : scopeLogs.getLogRecordsList()) {
                        try (XContentBuilder builder = jsonBuilder()) {
                            builder.startObject();
                            builder.field("_topic", record.topic());
                            builder.field("_partition", record.partition());
                            builder.field("_offset", record.offset());
                            OTelDocumentEncoder.toXContent(builder, resource, resourceSchemaURL);
                            OTelDocumentEncoder.toXContent(builder, scope, scopeSchemaURL);
                            OTelDocumentEncoder.toXContent(builder, logRecord);
                            builder.endObject();

                            // TODO optimise docID
                            final String docID = idPrefix + (documentOffset++);
                            final SourceToParse sourceToParse = new SourceToParse(
                                docID,
                                BytesReference.bytes(builder),
                                builder.contentType()
                            );
                            final Engine.Index op = IndexShard.prepareIndex(
                                getEngineConfig().getMapperService(),
                                sourceToParse,
                                UNASSIGNED_SEQ_NO,
                                getEngineConfig().getPrimaryTermSupplier().getAsLong(),
                                record.timestamp(),
                                VersionType.EXTERNAL_GTE,
                                Operation.Origin.PRIMARY,
                                UNSET_AUTO_GENERATED_TIMESTAMP,
                                true, // isRetry -- we may consume multiple times
                                UNASSIGNED_SEQ_NO,
                                0,
                                getEngineConfig().getRelativeTimeInNanosSupplier().getAsLong()
                            );
                            try {
                                Engine.IndexResult result = super.index(op);
                                Mapping update = op.parsedDoc().dynamicMappingsUpdate();
                                if (update != null) {
                                    // TODO this is surely not the right way to update mappings,
                                    // I just hacked this together to get something working.
                                    var indicesClient = client.admin().indices();
                                    var putMappingBuilder = indicesClient.preparePutMapping();
                                    var content = jsonBuilder();
                                    content.startObject();
                                    update.toXContent(content, ToXContent.EMPTY_PARAMS);
                                    content.endObject();
                                    putMappingBuilder.setSource(content);
                                    putMappingBuilder.setConcreteIndex(getEngineConfig().getShardId().getIndex());
                                    try {
                                        putMappingBuilder.execute().get();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        throw new RuntimeException(e);
                                    }
                                    result = super.index(op);
                                }

                                // TODO check result
                                // TODO can we support failure store with this approach?
                                /*
                                logger.info("created: " + result.isCreated());
                                logger.info("result type: " + result.getResultType());
                                logger.info("failure: " + result.getFailure());
                                logger.info("_id: " + result.getId());
                                logger.info("_seq_no: " + result.getSeqNo());
                                logger.info("_primary_term: " + result.getTerm());
                                logger.info("translog location: " + result.getTranslogLocation());
                                logger.info("required mapping update: " + result.getRequiredMappingUpdate());
                                 */

                            } catch (IOException e) {
                                // TODO
                                logger.error(e);
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        }
        try {
            // TODO commit offsets according to indexing results
            consumer.commitSync();
            return true;
        } catch (WakeupException e) {
            consumer.commitSync();
            return false; // shut down
        }
    }
}

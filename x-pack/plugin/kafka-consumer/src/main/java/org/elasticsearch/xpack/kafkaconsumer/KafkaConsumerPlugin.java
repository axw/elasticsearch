/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kafkaconsumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class KafkaConsumerPlugin extends Plugin implements EnginePlugin {
    private static final Logger logger = LogManager.getLogger(KafkaConsumerPlugin.class);

    public static final String THREAD_POOL_NAME = "kafka_consumer";
    private Client client;

    public KafkaConsumerPlugin(Settings settings) {}

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (false == indexSettings.getValue(KafkaEngine.INDEX_KAFKA_TOPIC).isEmpty()) {
            return Optional.of(config -> {
                KafkaEngine engine = new KafkaEngine(config, client);
                engine.start();
                return engine;
            });
        }
        return Optional.empty();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            KafkaEngine.KAFKA_BOOTSTRAP_SERVERS,
            KafkaEngine.INDEX_KAFKA_TOPIC,
            KafkaEngine.INDEX_KAFKA_KEY_DESERIALIZER,
            KafkaEngine.INDEX_KAFKA_VALUE_DESERIALIZER
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(
            new ScalingExecutorBuilder(THREAD_POOL_NAME, 0, 10, TimeValue.timeValueMinutes(1), true, "xpack.kafka_consumer.thread_pool")
        );
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        logger.info("Kafka consumer plugin is enabled");
        client = services.client();

        // TODO only run consumers on ingest/data nodes.

        // TODO enable dynamic configuration of Kafka clients and consumers.
        //
        // Users should be able to dynamically manage multiple Kafka clients, with:
        // standard client configuration settings defined at
        // https://docs.confluent.io/platform/7.8/installation/configuration/consumer-configs.html
        // Note that settings related to individual consumer groups (e.g. group.id)
        // must not be defined here.
        //
        // Users should then be able to dynamically manage multiple Kafka consumers,
        // referencing clients, to index documents from messages in topics. This
        // could be done by registering settings on a data stream. If data for one
        // topic should be written to multiple data streams, then the consumer must
        // be defined for one data stream and routed to others with an ingest processor.
        //
        // We should then start/stop consumers dynamically.
        final ClusterService clusterService = services.clusterService();
        final Settings settings = services.environment().settings();
        Properties clientConfig = new Properties();
        clientConfig.put("client.id", settings.get("xpack.kafka_consumer.client_id", services.nodeEnvironment().nodeId()));
        clientConfig.put("bootstrap.servers", settings.get("xpack.kafka_consumer.bootstrap_servers", "localhost:9092"));

        return Collections.emptyList();

        //return Collections.singleton(
        //    new KafkaConsumerManager(clientConfig, clusterService, services.threadPool().executor(THREAD_POOL_NAME), services.client())
        //);
    }
}

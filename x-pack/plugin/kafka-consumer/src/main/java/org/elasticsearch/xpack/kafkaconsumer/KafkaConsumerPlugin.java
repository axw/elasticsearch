/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kafkaconsumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerPlugin extends Plugin {
    private static final Logger logger = LogManager.getLogger(KafkaConsumerPlugin.class);

    private static final String  THREAD_POOL_NAME = "kafka_consumer";

    public KafkaConsumerPlugin(Settings settings) {}

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(
            new ScalingExecutorBuilder(
            THREAD_POOL_NAME,
            0,
            10,
            TimeValue.timeValueMinutes(1),
            true,
            "xpack.kafka_consumer.thread_pool"
        ));
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        logger.info("Kafka consumer plugin is enabled");

        // TODO only run consumers on data nodes.

        // TODO enable dynamic configuration of Kafka clients and consumers.
        //
        // Users should be able to dynamically manage multiple Kafka clients, with:
        // standard client configuration settings defined at https://docs.confluent.io/platform/7.8/installation/configuration/consumer-configs.html
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

        return Collections.singleton(new KafkaConsumerManager(
            clientConfig,
            clusterService,
            services.threadPool().executor(THREAD_POOL_NAME)
        ));
    }
}

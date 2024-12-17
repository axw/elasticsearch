/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

public class KafkaConsumerManager extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(KafkaConsumerManager.class);

    private final Properties clientConfig;
    private final ClusterService clusterService;
    private final ExecutorService executorService;

    public KafkaConsumerManager(Properties clientConfig, ClusterService clusterService, ExecutorService executorService) {
        this.clientConfig = clientConfig;
        this.clusterService = clusterService;
        this.executorService = executorService;
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
            consumerConfig.put("value.deserializer", Class.forName("org.apache.kafka.common.serialization.StringDeserializer"));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        executorService.submit(new KafkaConsumerRunnable(
            consumerConfig,
            Pattern.compile("otlp-.*")
        ));
    }

    @Override
    protected void doStop() {
        // TODO initiate executor shutdown, wake up consumers, wait for them to stop
    }

    @Override
    protected void doClose() throws IOException {
        executorService.close();
    }

    private static class KafkaConsumerRunnable implements Runnable {
        private static final Logger logger = LogManager.getLogger(KafkaConsumerRunnable.class);

        private final Properties consumerConfig;
        private final Pattern topicPattern;
        private final CountDownLatch shutdownLatch;
        private boolean isShutdown;
        private KafkaConsumer<String, String> consumer;

        KafkaConsumerRunnable(Properties consumerConfig, Pattern topicPattern) {
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
            final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
            try {
                consumer.subscribe(topicPattern);
                synchronized (this) {
                    if (!isShutdown) {
                        this.consumer = consumer;
                    }
                }
                while (!shuttingDown) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(1));
                    records.forEach(record -> process(record));
                    try {
                        consumer.commitSync(); // commit after: at-least-once delivery
                    } catch (WakeupException e) {
                        shuttingDown = true;
                        consumer.commitSync();
                    }
                }
            } catch (WakeupException e) {
                shuttingDown = true;
            } catch (Exception e) {
                throw e;
            } finally {
                consumer.close();
                shutdownLatch.countDown();
            }
        }

        private void process(ConsumerRecord<String, String> record) {
            logger.info("process record", record);
        }
    }
}

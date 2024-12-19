/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * ShardPartitionAssignor is a custom ConsumerPartitionAssignor that assigns
 * partitions to the corresponding shard ID's group member. Each group is for
 * a single index (but not necessarily a single _topic_), and each group member
 * can be identified by its shard ID.
 *
 * Note that the assignor WILL NOT assign partitions that lack a corresponding
 * shard ID.
 */
public class ShardPartitionAssignor implements ConsumerPartitionAssignor {
    private static final Logger logger = LogManager.getLogger(ShardPartitionAssignor.class);

    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        logger.info("partition assigned: assignment=" + assignment + ", metadata=" + metadata);
    }

    @Override
    public GroupAssignment assign(Cluster cluster, GroupSubscription groupSubscription) {
        // Map shard ID (group instance ID) to its member ID.
        // Record topics that are being consumed.
        final Set<String> topics = new HashSet<>();
        final Map<Integer, String> shardMembers = new HashMap<>();
        for (var entry : groupSubscription.groupSubscription().entrySet()) {
            var memberId = entry.getKey();
            var subscription = entry.getValue();
            var shardId = Integer.parseInt(subscription.groupInstanceId().orElse("-1"));
            shardMembers.put(shardId, memberId);
            topics.addAll(subscription.topics());
        }

        // Each member _must_ have an assignment, even if empty (i.e. because there
        // are more shards than partitions -- something that we should ideally make
        // impossible.)
        final Map<String, Assignment> assignments = new HashMap<>();
        for (var memberId : shardMembers.values()) {
            assignments.put(memberId, new Assignment(new ArrayList<>()));
        }

        for (String topic : topics) {
            for (PartitionInfo partition : cluster.partitionsForTopic(topic)) {
                String memberId = shardMembers.get(partition.partition());
                if (memberId == null) {
                    // TODO we need to create another shard: either split
                    // the index, or roll over to a new index.
                    logger.info("no corresponding shard for " + partition);
                    continue;
                }
                Assignment assignment = assignments.get(memberId);
                assignment.partitions().add(new TopicPartition(topic, partition.partition()));
            }
        }
        return new GroupAssignment(assignments);
    }

    @Override
    public String name() {
        return "elasticsearch";
    }
}

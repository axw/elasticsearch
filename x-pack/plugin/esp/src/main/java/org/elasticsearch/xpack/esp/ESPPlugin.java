/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class ESPPlugin extends Plugin implements ActionPlugin {
    private static final Logger logger = LogManager.getLogger(ESPPlugin.class);

    private static final Map<IndexShardKey, IndexShard> shardMap = new HashMap<IndexShardKey, IndexShard>();
    public ESPPlugin(Settings settings) {
        super();
    }

    public static IndexShard getIndexShard(IndexShardKey key) {
        synchronized (shardMap) {
            return shardMap.get(key);
        }
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(ESPIndexAction.INSTANCE, TransportESPIndexAction.class)
        );
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(
            new ESPRestHandler()
        );
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        indexModule.addIndexEventListener(new IndexEventListener() {
            @Override
            public void afterIndexShardStarted(IndexShard indexShard) {
                final ShardId shardId = indexShard.shardId();
                final IndexShardKey key = new IndexShardKey(shardId.getIndexName(), shardId.id());
                logger.info("IndexShard started: " + key);
                synchronized (shardMap) {
                    shardMap.put(key, indexShard);
                }
            }

            @Override
            public void afterIndexShardClosing(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
                final IndexShardKey key = new IndexShardKey(shardId.getIndexName(), shardId.id());
                logger.info("IndexShard closing: " + key);
                synchronized (shardMap) {
                    shardMap.remove(key);
                }
            }
        });
    }

    /*
    // Factory for the custom Engine
    public static class BatchedLuceneCommitEngineFactory implements EngineFactory {
        @Override
        public Engine newReadWriteEngine(EngineConfig config) {
            return new BatchedLuceneCommitEngine(config);
        }
    }

    // EnginePlugin: register the custom Engine for a special index setting
    @Override
    public Optional<EngineFactory> getEngineFactory(org.elasticsearch.index.IndexSettings indexSettings) {
        if (indexSettings.getSettings().getAsBoolean("index.esp.batched_lucene_commit", false)) {
            return Optional.of(new BatchedLuceneCommitEngineFactory());
        }
        return Optional.empty();
    }
    */
}

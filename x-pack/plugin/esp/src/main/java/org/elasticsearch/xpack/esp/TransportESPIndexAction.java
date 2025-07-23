/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esp;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.BulkShardResponse;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.lang.reflect.Executable;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

public class TransportESPIndexAction extends TransportAction<ESPIndexRequest, ESPIndexResponse> {
    private static final Logger logger = LogManager.getLogger(TransportESPIndexAction.class);

    private final TransportService transportService;
    private final UpdateHelper updateHelper;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final ExecutorSelector executorSelector;
    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject
    @SuppressWarnings("this-escape")
    public TransportESPIndexAction(
        TransportService transportService,
        ActionFilters actionFilters,
        UpdateHelper updateHelper,
        ThreadPool threadPool,
        ClusterService clusterService,
        SystemIndices systemIndices,
        MappingUpdatedAction mappingUpdatedAction
    ) {
        super(ESPIndexAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.updateHelper = updateHelper;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.executorSelector = systemIndices.getExecutorSelector();
        this.mappingUpdatedAction = mappingUpdatedAction;
    }

    @Override
    protected void doExecute(Task task, ESPIndexRequest request, ActionListener<ESPIndexResponse> listener) {
        assert task instanceof CancellableTask;
        try {
            ESPIndexResponse response = indexData((CancellableTask)task, request);
            listener.onResponse(response);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private ESPIndexResponse indexData(CancellableTask task, ESPIndexRequest request) throws Exception {
        Arrays.stream(request.getIndexShardRecords()).forEach(indexShardRecords -> {
            final IndexShardKey key = indexShardRecords.getKey();
            final IndexShard indexShard = ESPPlugin.getIndexShard(key);
            if (indexShard == null) {
                throw new RuntimeException("index shard not found for key: " + key);
            }

            final ClusterStateObserver clusterStateObserver = new ClusterStateObserver(
                    clusterService,
                    TimeValue.ONE_MINUTE, // TODO make this configurable
                    logger, threadPool.getThreadContext()
            );

            final BiFunction<ExecutorSelector, IndexShard, Executor> executorFunction = ExecutorSelector.getWriteExecutorForShard(threadPool);
            final Executor executor = executorFunction.apply(executorSelector, indexShard);

            final ESPIndexRequest.Record[] records = indexShardRecords.getRecords();
            final BulkItemRequest[] bulkitemRequests = new BulkItemRequest[records.length];
            for (int i = 0; i < records.length; i++) {
                final ESPIndexRequest.Record record = records[i];
                final IndexRequest indexRequest = new IndexRequest(key.getIndexName());
                indexRequest.opType(DocWriteRequest.OpType.CREATE);
                indexRequest.id(record.getId());
                indexRequest.source(record.getSource());
                bulkitemRequests[i] = new BulkItemRequest(i, indexRequest);
            }

            final BulkShardRequest bulkRequest = new BulkShardRequest(
                    indexShard.shardId(),
                    WriteRequest.RefreshPolicy.WAIT_UNTIL, // TODO execute bulk requests for each shard in parallel
                    bulkitemRequests
            );

            TransportShardBulkAction.performOnPrimary(
                    bulkRequest,
                    indexShard,
                    updateHelper,
                    threadPool::absoluteTimeInMillis,
                    (update, shardId, mappingListener) -> {
                        assert update != null;
                        assert shardId != null;
                        mappingUpdatedAction.updateMappingOnMaster(shardId.getIndex(), update, mappingListener);
                    },
                    (mappingUpdateListener, initialMappingVersion) -> clusterStateObserver.waitForNextChange(
                            new ClusterStateObserver.Listener() {
                                @Override
                                public void onNewClusterState(ClusterState state) {
                                    mappingUpdateListener.onResponse(null);
                                }
                                @Override
                                public void onClusterServiceClose() {
                                    mappingUpdateListener.onFailure(new NodeClosedException(clusterService.localNode()));
                                }
                                @Override
                                public void onTimeout(TimeValue timeout) {
                                    mappingUpdateListener.onFailure(new MapperException("timed out while waiting for a dynamic mapping update"));
                                }
                            }
                    ),
                    new ActionListener<TransportReplicationAction.PrimaryResult<BulkShardRequest, BulkShardResponse>>() {
                        @Override
                        public void onResponse(TransportReplicationAction.PrimaryResult<BulkShardRequest, BulkShardResponse> result) {
                            logger.info("onResponse: " + result);
                        }
                        @Override
                        public void onFailure(Exception e) {
                            logger.error("onFailure: " + e);
                        }
                    },
                    executor
            );

            //indexShardData.applyIndexOperationOnPrimary();
            //ShardId shardId = indexShardData.getShardId();

            //ESPPlugin.getIndexShard()

            //indexShard.applyIndexOperationOnPrimary();
            //indexShard.flush();
        });

        //request.

        // indexShard.applyIndexOperationOnPrimary()
        // indexShard.flush()
        return new ESPIndexResponse();
    }
}

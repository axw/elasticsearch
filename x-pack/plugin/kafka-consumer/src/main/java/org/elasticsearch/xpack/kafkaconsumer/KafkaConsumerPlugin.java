/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kafkaconsumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class KafkaConsumerPlugin extends Plugin implements PersistentTaskPlugin {
    private static final Logger logger = LogManager.getLogger(KafkaConsumerPlugin.class);

    public KafkaConsumerPlugin(Settings settings) {
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        return Collections.emptyList();
    }

    /*
    @Override
    public Collection<?> createComponents(PluginServices services) {
        logger.info("APM ingest plugin is {}", enabled ? "enabled" : "disabled");
        Settings settings = services.environment().settings();
        ClusterService clusterService = services.clusterService();
        registry.set(
            new APMIndexTemplateRegistry(settings, clusterService, services.threadPool(), services.client(), services.xContentRegistry())
        );
        if (enabled) {
            APMIndexTemplateRegistry registryInstance = registry.get();
            registryInstance.setEnabled(APM_DATA_REGISTRY_ENABLED.get(settings));
            registryInstance.initialize();
        }
        return Collections.emptyList();
    }
    */

    //@Override
    //public void close() {
    //registry.get().
    //close();
    //}
}

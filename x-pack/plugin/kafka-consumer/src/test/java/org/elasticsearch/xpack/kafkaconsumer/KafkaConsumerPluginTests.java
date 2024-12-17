/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kafkaconsumer;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaConsumerPluginTests extends ESTestCase {
    private KafkaConsumerPlugin plugin;
    private ClusterService clusterService;
    private ThreadPool threadPool;

    @Before
    public void createPlugin() {
        final ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream().collect(Collectors.toSet())
        );
        threadPool = new TestThreadPool(this.getClass().getName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings);
        plugin = new KafkaConsumerPlugin(Settings.builder().build());
    }

    private KafkaConsumerManager createComponents() {
        Environment mockEnvironment = mock(Environment.class);
        when(mockEnvironment.settings()).thenReturn(Settings.builder().build());
        Plugin.PluginServices services = mock(Plugin.PluginServices.class);
        when(services.clusterService()).thenReturn(clusterService);
        when(services.threadPool()).thenReturn(threadPool);
        when(services.environment()).thenReturn(mockEnvironment);
        Collection<?> components = plugin.createComponents(services);
        assertThat(components, hasSize(1));
        return (KafkaConsumerManager) components.iterator().next();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        plugin.close();
        threadPool.shutdownNow();
    }

    public void testTodo() {}
}

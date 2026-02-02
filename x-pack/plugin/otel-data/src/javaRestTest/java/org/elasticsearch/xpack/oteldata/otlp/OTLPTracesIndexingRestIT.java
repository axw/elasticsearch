/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;

public class OTLPTracesIndexingRestIT extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";
    private static final Resource TEST_RESOURCE = Resource.create(Attributes.of(stringKey("service.name"), "elasticsearch"));

    private OtlpHttpSpanExporter exporter;
    private SdkTracerProvider tracerProvider;
    private Tracer tracer;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .user(USER, PASS, "superuser", false)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void beforeTest() throws Exception {
        exporter = OtlpHttpSpanExporter.builder()
            .setEndpoint(getClusterHosts().getFirst().toURI() + "/_otlp/v1/traces")
            .addHeader("Authorization", "ApiKey " + createApiKey())
            .build();
        tracerProvider = SdkTracerProvider.builder()
            .setResource(TEST_RESOURCE)
            .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
            .build();
        tracer = tracerProvider.get("io.opentelemetry.example.traces");
        assertBusy(() -> assertOK(client().performRequest(new Request("GET", "_index_template/traces-otel@template"))));
    }

    private static String createApiKey() throws IOException {
        Request createApiKeyRequest = new Request("POST", "/_security/api_key");
        createApiKeyRequest.setJsonEntity("""
            {
              "name": "otel-traces-test-key",
              "role_descriptors": {
                "traces_writer": {
                  "index": [
                    {
                      "names": ["traces-*"],
                      "privileges": ["create_doc", "auto_configure"]
                    }
                  ]
                }
              }
            }
            """);
        ObjectPath createApiKeyResponse = ObjectPath.createFromResponse(client().performRequest(createApiKeyRequest));
        return createApiKeyResponse.evaluate("encoded");
    }

    @Override
    public void tearDown() throws Exception {
        if (tracerProvider != null) {
            tracerProvider.close();
        }
        super.tearDown();
    }

    public void testIngestSpanViaOtlp() throws Exception {
        io.opentelemetry.api.trace.Span span = tracer.spanBuilder("test-span")
            .setSpanKind(SpanKind.SERVER)
            .setAttribute(AttributeKey.stringKey("http.method"), "GET")
            .startSpan();
        span.end();
        tracerProvider.forceFlush().join(10, java.util.concurrent.TimeUnit.SECONDS);

        refreshTracesIndices();

        ObjectPath search = search("traces-generic.otel-default");
        assertThat(search.evaluate("hits.total.value"), equalTo(1));
        Object source = search.evaluate("hits.hits.0._source");
        assertThat(ObjectPath.evaluate(source, "@timestamp"), isA(String.class));
        assertThat(ObjectPath.evaluate(source, "name"), equalTo("test-span"));
        assertThat(ObjectPath.evaluate(source, "kind"), equalTo("SERVER"));
        assertThat(ObjectPath.evaluate(source, "trace_id"), isA(String.class));
        assertThat(ObjectPath.evaluate(source, "span_id"), isA(String.class));
        assertThat(ObjectPath.evaluate(source, "resource.attributes.service\\.name"), equalTo("elasticsearch"));
        assertThat(ObjectPath.evaluate(source, "data_stream.type"), equalTo("traces"));
        assertThat(ObjectPath.evaluate(source, "data_stream.dataset"), equalTo("generic.otel"));
        assertThat(ObjectPath.evaluate(source, "data_stream.namespace"), equalTo("default"));
    }

    private void refreshTracesIndices() throws IOException {
        assertOK(client().performRequest(new Request("POST", "traces-*/_refresh")));
    }

    private ObjectPath search(String index) throws IOException {
        return ObjectPath.createFromResponse(client().performRequest(new Request("GET", index + "/_search")));
    }
}

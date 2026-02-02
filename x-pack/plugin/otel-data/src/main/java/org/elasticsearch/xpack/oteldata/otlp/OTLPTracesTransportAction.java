/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.TraceDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Transport action for handling OpenTelemetry Protocol (OTLP) Traces requests.
 * Indexes one document per span into traces data streams, compatible with the
 * elasticsearchexporter otel mapping mode.
 *
 * @see <a href="https://opentelemetry.io/docs/specs/otlp">OTLP Specification</a>
 */
public class OTLPTracesTransportAction extends HandledTransportAction<
    OTLPTracesTransportAction.TracesRequest,
    OTLPTracesTransportAction.TracesResponse> {

    public static final String NAME = "indices:data/write/otlp/traces";
    public static final ActionType<TracesResponse> TYPE = new ActionType<>(NAME);

    private static final Logger logger = LogManager.getLogger(OTLPTracesTransportAction.class);

    private final Client client;

    @Inject
    public OTLPTracesTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client
    ) {
        super(NAME, transportService, actionFilters, TracesRequest::new, threadPool.executor(ThreadPool.Names.WRITE));
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, TracesRequest request, ActionListener<TracesResponse> listener) {
        BufferedByteStringAccessor byteStringAccessor = new BufferedByteStringAccessor();
        TraceDocumentBuilder documentBuilder = new TraceDocumentBuilder(byteStringAccessor);
        try {
            ExportTraceServiceRequest traceRequest = ExportTraceServiceRequest.parseFrom(request.exportTraceServiceRequest.streamInput());
            int totalSpans = countSpans(traceRequest);
            if (totalSpans == 0) {
                handleEmptyRequest(listener);
                return;
            }
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            for (ResourceSpans resourceSpans : traceRequest.getResourceSpansList()) {
                Resource resource = resourceSpans.getResource();
                for (ScopeSpans scopeSpans : resourceSpans.getScopeSpansList()) {
                    InstrumentationScope scope = scopeSpans.getScope();
                    String scopeName = scope.getName().isEmpty() ? null : scope.getName();
                    for (Span span : scopeSpans.getSpansList()) {
                        try {
                            TargetIndex targetIndex = TargetIndex.evaluate(
                                TargetIndex.TYPE_TRACES,
                                span.getAttributesList(),
                                scopeName,
                                scope.getAttributesList(),
                                resource.getAttributesList()
                            );
                            try (XContentBuilder xContentBuilder = XContentFactory.cborBuilder(new BytesStreamOutput())) {
                                String indexName = documentBuilder.buildSpanDocument(
                                    xContentBuilder,
                                    resource,
                                    scope,
                                    span,
                                    targetIndex
                                );
                                bulkRequestBuilder.add(
                                    new IndexRequest(indexName).setRequireDataStream(true)
                                        .source(xContentBuilder)
                                        .setIncludeSourceOnError(false)
                                );
                            }
                        } catch (Exception e) {
                            logger.warn("failed to build span document", e);
                            // Skip this span; we'll report partial success
                        }
                    }
                }
            }
            if (bulkRequestBuilder.numberOfActions() == 0) {
                handlePartialSuccess(listener, totalSpans, "all spans failed validation or document build");
                return;
            }
            bulkRequestBuilder.execute(new ActionListener<>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    if (bulkResponse.hasFailures()) {
                        handlePartialSuccess(bulkResponse, totalSpans, listener);
                    } else if (bulkResponse.getItems().length < totalSpans) {
                        int rejected = totalSpans - bulkResponse.getItems().length;
                        handlePartialSuccess(listener, rejected, "some spans were skipped during document build");
                    } else {
                        handleSuccess(listener);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    handleFailure(listener, e, totalSpans);
                }
            });
        } catch (Exception e) {
            logger.error("failed to execute otlp traces request", e);
            handleFailure(listener, e, 0);
        }
    }

    private static int countSpans(ExportTraceServiceRequest request) {
        int count = 0;
        for (ResourceSpans rs : request.getResourceSpansList()) {
            for (ScopeSpans ss : rs.getScopeSpansList()) {
                count += ss.getSpansCount();
            }
        }
        return count;
    }

    private static void handleSuccess(ActionListener<TracesResponse> listener) {
        listener.onResponse(new TracesResponse(RestStatus.OK, ExportTraceServiceResponse.newBuilder().build()));
    }

    private static void handleEmptyRequest(ActionListener<TracesResponse> listener) {
        // If the server receives an empty request, the server SHOULD respond with success.
        // https://opentelemetry.io/docs/specs/otlp/#full-success-1
        handleSuccess(listener);
    }

    private static void handlePartialSuccess(ActionListener<TracesResponse> listener, int rejectedSpans, String message) {
        ExportTraceServiceResponse response = ExportTraceServiceResponse.newBuilder()
            .setPartialSuccess(
                ExportTracePartialSuccess.newBuilder()
                    .setRejectedSpans(rejectedSpans)
                    .setErrorMessage(message)
                    .build()
            )
            .build();
        listener.onResponse(new TracesResponse(RestStatus.OK, response));
    }

    private static void handlePartialSuccess(
        BulkResponse bulkResponse,
        int totalSpans,
        ActionListener<TracesResponse> listener
    ) {
        Map<String, Map<RestStatus, FailureGroup>> failureGroups = Maps.newHashMapWithExpectedSize(4);
        RestStatus status = RestStatus.OK;
        int failures = 0;
        for (BulkItemResponse item : bulkResponse.getItems()) {
            BulkItemResponse.Failure failure = item.getFailure();
            if (failure != null) {
                failures++;
                if (failure.getStatus() == RestStatus.TOO_MANY_REQUESTS) {
                    status = RestStatus.TOO_MANY_REQUESTS;
                }
                failureGroups.computeIfAbsent(failure.getIndex(), k -> new HashMap<>())
                    .computeIfAbsent(failure.getStatus(), k -> new FailureGroup(new AtomicInteger(0), failure.getMessage()))
                    .failureCount().incrementAndGet();
            }
        }
        StringBuilder msg = new StringBuilder();
        for (Map.Entry<String, Map<RestStatus, FailureGroup>> indexEntry : failureGroups.entrySet()) {
            for (Map.Entry<RestStatus, FailureGroup> statusEntry : indexEntry.getValue().entrySet()) {
                FailureGroup g = statusEntry.getValue();
                msg.append("Index [")
                    .append(indexEntry.getKey())
                    .append("] returned status [")
                    .append(statusEntry.getKey())
                    .append("] for ")
                    .append(g.failureCount().get())
                    .append(" documents. Sample error: ")
                    .append(g.failureMessageSample())
                    .append("\n");
            }
        }
        ExportTraceServiceResponse response = ExportTraceServiceResponse.newBuilder()
            .setPartialSuccess(
                ExportTracePartialSuccess.newBuilder()
                    .setRejectedSpans(failures)
                    .setErrorMessage(msg.toString())
                    .build()
            )
            .build();
        listener.onResponse(new TracesResponse(status, response));
    }

    record FailureGroup(AtomicInteger failureCount, String failureMessageSample) {}

    private static void handleFailure(ActionListener<TracesResponse> listener, Exception e, int totalSpans) {
        RestStatus restStatus = ExceptionsHelper.status(e);
        ExportTraceServiceResponse response = ExportTraceServiceResponse.newBuilder()
            .setPartialSuccess(
                ExportTracePartialSuccess.newBuilder()
                    .setRejectedSpans(totalSpans)
                    .setErrorMessage(e.getMessage())
                    .build()
            )
            .build();
        listener.onResponse(new TracesResponse(restStatus, response));
    }

    public static class TracesRequest extends ActionRequest implements CompositeIndicesRequest {
        private final BytesReference exportTraceServiceRequest;

        public TracesRequest(StreamInput in) throws IOException {
            super(in);
            exportTraceServiceRequest = in.readBytesReference();
        }

        public TracesRequest(BytesReference exportTraceServiceRequest) {
            this.exportTraceServiceRequest = exportTraceServiceRequest;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBytesReference(exportTraceServiceRequest);
        }
    }

    public static class TracesResponse extends ActionResponse {
        private final BytesReference response;
        private final RestStatus status;

        public TracesResponse(RestStatus status, ExportTraceServiceResponse response) {
            this(status, new BytesArray(response.toByteArray()));
        }

        public TracesResponse(RestStatus status, BytesReference response) {
            this.response = response;
            this.status = status;
        }

        public TracesResponse(StreamInput in) throws IOException {
            super();
            response = in.readBytesReference();
            status = in.readEnum(RestStatus.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesReference(response);
            out.writeEnum(status);
        }

        public BytesReference getResponse() {
            return response;
        }

        public RestStatus getStatus() {
            return status;
        }
    }
}

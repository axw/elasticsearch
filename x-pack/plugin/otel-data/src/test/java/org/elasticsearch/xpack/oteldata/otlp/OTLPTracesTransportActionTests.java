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
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;

import com.google.protobuf.ByteString;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.oteldata.otlp.OTLPTracesTransportAction.TracesResponse;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OTLPTracesTransportActionTests extends ESTestCase {

    private OTLPTracesTransportAction action;
    private Client client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        when(client.prepareBulk()).thenAnswer(invocation -> new BulkRequestBuilder(client));

        action = new OTLPTracesTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(ThreadPool.class),
            client
        );
    }

    public void testSuccess() throws Exception {
        TracesResponse response = executeRequest(createTracesRequest(createSpan()));

        assertThat(response.getStatus(), equalTo(RestStatus.OK));
        ExportTraceServiceResponse traceResponse = ExportTraceServiceResponse.parseFrom(response.getResponse().array());
        assertThat(traceResponse.hasPartialSuccess(), equalTo(false));
    }

    public void testSuccessEmptyRequest() throws Exception {
        TracesResponse response = executeRequest(createTracesRequest());

        assertThat(response.getStatus(), equalTo(RestStatus.OK));
        ExportTraceServiceResponse traceResponse = ExportTraceServiceResponse.parseFrom(response.getResponse().array());
        assertThat(traceResponse.hasPartialSuccess(), equalTo(false));
    }

    public void test429() throws Exception {
        BulkItemResponse[] bulkItemResponses = new BulkItemResponse[] {
            failureResponse("traces-generic.otel-default", RestStatus.TOO_MANY_REQUESTS, "too many requests"),
            successResponse() };
        TracesResponse response = executeRequest(createTracesRequest(createSpan()), new BulkResponse(bulkItemResponses, 0));

        assertThat(response.getStatus(), equalTo(RestStatus.TOO_MANY_REQUESTS));
        ExportTracePartialSuccess partial = ExportTraceServiceResponse.parseFrom(response.getResponse().array())
            .getPartialSuccess();
        assertThat(
            partial.getRejectedSpans(),
            equalTo(Arrays.stream(bulkItemResponses).filter(BulkItemResponse::isFailed).count())
        );
        assertThat(partial.getErrorMessage(), containsString("too many requests"));
    }

    public void testPartialSuccess() throws Exception {
        TracesResponse response = executeRequest(
            createTracesRequest(createSpan()),
            new BulkResponse(
                new BulkItemResponse[] {
                    failureResponse("traces-generic.otel-default", RestStatus.BAD_REQUEST, "bad request 1"),
                    failureResponse("traces-generic.otel-default", RestStatus.BAD_REQUEST, "bad request 2"),
                    failureResponse("traces-generic.otel-default", RestStatus.INTERNAL_SERVER_ERROR, "internal server error") },
                0
            )
        );

        assertThat(response.getStatus(), equalTo(RestStatus.OK));
        ExportTracePartialSuccess partial = ExportTraceServiceResponse.parseFrom(response.getResponse().array())
            .getPartialSuccess();
        assertThat(partial.getRejectedSpans(), equalTo(3L));
        assertThat(partial.getErrorMessage(), containsString("bad request 1"));
        assertThat(partial.getErrorMessage(), not(containsString("bad request  2")));
        assertThat(partial.getErrorMessage(), containsString("internal server error"));
    }

    public void testBulkError() throws Exception {
        assertExceptionStatus(new IllegalArgumentException("bazinga"), RestStatus.BAD_REQUEST);
        assertExceptionStatus(new IllegalStateException("bazinga"), RestStatus.INTERNAL_SERVER_ERROR);
    }

    private void assertExceptionStatus(Exception exception, RestStatus restStatus) throws Exception {
        doThrow(exception).when(client).execute(any(), any(), any());
        TracesResponse response = executeRequest(createTracesRequest(createSpan()), exception);

        assertThat(response.getStatus(), equalTo(restStatus));
        ExportTracePartialSuccess partial = ExportTraceServiceResponse.parseFrom(response.getResponse().array())
            .getPartialSuccess();
        assertThat(partial.getRejectedSpans(), equalTo(1L));
        assertThat(partial.getErrorMessage(), equalTo(exception.getMessage()));
    }

    private TracesResponse executeRequest(OTLPTracesTransportAction.TracesRequest request) {
        return executeRequest(request, listener -> listener.onResponse(new BulkResponse(new BulkItemResponse[] { successResponse() }, 0)));
    }

    private TracesResponse executeRequest(OTLPTracesTransportAction.TracesRequest request, BulkResponse bulkResponse) {
        return executeRequest(request, listener -> listener.onResponse(bulkResponse));
    }

    private TracesResponse executeRequest(OTLPTracesTransportAction.TracesRequest request, Exception bulkFailure) {
        return executeRequest(request, listener -> listener.onFailure(bulkFailure));
    }

    private TracesResponse executeRequest(
        OTLPTracesTransportAction.TracesRequest request,
        Consumer<ActionListener<BulkResponse>> bulkResponseConsumer
    ) {
        ArgumentCaptor<ActionListener<BulkResponse>> bulkResponseListener = ArgumentCaptor.captor();
        doNothing().when(client).execute(any(), any(), bulkResponseListener.capture());

        @SuppressWarnings("unchecked")
        ActionListener<TracesResponse> responseListener = mock(ActionListener.class);
        action.doExecute(null, request, responseListener);
        if (bulkResponseListener.getAllValues().isEmpty() == false) {
            bulkResponseConsumer.accept(bulkResponseListener.getValue());
        }

        ArgumentCaptor<TracesResponse> response = ArgumentCaptor.forClass(TracesResponse.class);
        verify(responseListener).onResponse(response.capture());
        return response.getValue();
    }

    private static OTLPTracesTransportAction.TracesRequest createTracesRequest(Span... spans) {
        ExportTraceServiceRequest request;
        if (spans.length == 0) {
            request = ExportTraceServiceRequest.newBuilder().build();
        } else {
            ResourceSpans resourceSpans = ResourceSpans.newBuilder()
                .setResource(Resource.newBuilder().addAllAttributes(List.of(keyValue("service.name", "test-service"))).build())
                .addScopeSpans(
                    ScopeSpans.newBuilder()
                        .setScope(InstrumentationScope.newBuilder().setName("test-scope").build())
                        .addAllSpans(List.of(spans))
                        .build()
                )
                .build();
            request = ExportTraceServiceRequest.newBuilder().addResourceSpans(resourceSpans).build();
        }
        return new OTLPTracesTransportAction.TracesRequest(new BytesArray(request.toByteArray()));
    }

    private static Span createSpan() {
        byte[] traceId = new byte[16];
        byte[] spanId = new byte[8];
        for (int i = 0; i < 16; i++) traceId[i] = (byte) (i + 1);
        for (int i = 0; i < 8; i++) spanId[i] = (byte) (i + 1);
        long startNanos = 1_000_000_000L;
        long endNanos = 2_000_000_000L;
        return Span.newBuilder()
            .setTraceId(ByteString.copyFrom(traceId))
            .setSpanId(ByteString.copyFrom(spanId))
            .setName("test-span")
            .setKind(Span.SpanKind.SPAN_KIND_SERVER)
            .setStartTimeUnixNano(startNanos)
            .setEndTimeUnixNano(endNanos)
            .setStatus(Status.newBuilder().setCode(Status.StatusCode.STATUS_CODE_OK).build())
            .build();
    }

    private static BulkItemResponse successResponse() {
        return BulkItemResponse.success(-1, DocWriteRequest.OpType.CREATE, mock(DocWriteResponse.class));
    }

    private static BulkItemResponse failureResponse(String index, RestStatus restStatus, String failureMessage) {
        return BulkItemResponse.failure(
            -1,
            DocWriteRequest.OpType.CREATE,
            new BulkItemResponse.Failure(index, "id", new RuntimeException(failureMessage), restStatus)
        );
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestResponse.TEXT_CONTENT_TYPE;
import static org.elasticsearch.rest.RestStatus.OK;

public class ESPRestHandler extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(ESPRestHandler.class);

    @Override
    public String getName() {
        return "esp";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_esp"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ESPIndexRequest indexRequest = new ESPIndexRequest();
        // TODO investigate consuming request body as a stream, rather than parsing up front, and index into IndexShards as we go.
        request.applyContentParser(indexRequest::parseXContent);
        return channel -> {
            RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(
                ESPIndexAction.INSTANCE,
                indexRequest,
                new RestRefCountedChunkedToXContentListener<>(channel)
            );
        };
    }
}

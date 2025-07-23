/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esp;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ESPIndexRequest extends ActionRequest implements IndicesRequest {
    public static final ParseField DOCS_FIELD = new ParseField("docs");

    private ESPIndexRequest.IndexShardRecords[] indexShardRecords;

    public ESPIndexRequest() {
    }

    public ESPIndexRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public void parseXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = null;
        if (token != XContentParser.Token.START_OBJECT && (token = parser.nextToken()) != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Expected [" + XContentParser.Token.START_OBJECT + "] but found [" + token + "]."
            );
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            //if (token == XContentParser.Token.FIELD_NAME) {
            //
            //}
        }

        token = parser.nextToken();
        if (token != null) {
            throw new ParsingException(parser.getTokenLocation(), "Unexpected token [" + token + "] found after the main object.");
        }

        // TODO parse from request body
        this.indexShardRecords = new ESPIndexRequest.IndexShardRecords[]{
            new IndexShardRecords("index_name", 0, new Record[]{
                    new Record("1", Map.of("foo1", "bar1")),
                    new Record("2", Map.of("foo2", "bar2")),
                    new Record("3", Map.of("foo3", "bar3"))
            }),
            new IndexShardRecords("index_name", 1, new Record[]{
                    new Record("4", Map.of("foo4", "bar4")),
                    new Record("5", Map.of("foo5", "bar5")),
                    new Record("6", Map.of("foo6", "bar6"))
            }),
        };
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, null, parentTaskId, headers) {
            @Override
            public String getDescription() {
                return "esp:index task";
            }
        };
    }

    @Override
    public String[] indices() {
        return List.of(indexShardRecords).stream().map(
                indexShardData -> indexShardData.key.getIndexName())
                .distinct().toArray(String[]::new);
    }

    public IndexShardRecords[] getIndexShardRecords() {
        return indexShardRecords;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.builder()
                .build();
    }

    public class IndexShardRecords {
        private final IndexShardKey key;
        private final Record[] records;

        public IndexShardRecords(String indexName, int shardId, Record[] records) {
            this.key = new IndexShardKey(indexName, shardId);
            this.records = records;
        }

        public IndexShardKey getKey() {
            return key;
        }

        public Record[] getRecords() {
            return records;
        }
    }

    public class Record {
        private final String id;
        private final Map<String, ?> source;

        public Record(String id, Map<String, ?> source) {
            this.id = id;
            this.source = source;
        }

        public String getId() {
            return id;
        }

        public Map<String, ?> getSource() {
            return source;
        }
    }
}

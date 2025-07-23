/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esp;

public class IndexShardKey {
    private final String indexName;
    private final int shardId;

    public IndexShardKey(String indexName, int shardId) {
        this.indexName = indexName;
        this.shardId = shardId;
    }

    public String getIndexName() {
        return indexName;
    }

    public int getShardId() {
        return shardId;
    }

    public String toString() {
        return indexName + "[" + shardId + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexShardKey that = (IndexShardKey) o;
        return shardId == that.shardId && indexName.equals(that.indexName);
    }

    @Override
    public int hashCode() {
        int result = indexName.hashCode();
        result = 31 * result + shardId;
        return result;
    }
}


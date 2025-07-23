/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esp;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

public class ESPIndexAction extends ActionType<ESPIndexResponse> {
    public static final ESPIndexAction INSTANCE = new ESPIndexAction();
    public static final String NAME = "indices:data/write/esp";

    public ESPIndexAction() {
        super(NAME);
    }
}

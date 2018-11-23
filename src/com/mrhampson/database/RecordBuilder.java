/*
 * RecordBuilder.java
 * Created on Nov 23, 2018, 3:47 PM
 *
 * Copyright 2008-2018 LiveAction, Incorporated. All Rights Reserved.
 * 3500 W Bayshore Road, Palo Alto, California 94303, U.S.A.
 *
 * This software is the confidential and proprietary information
 * of LiveAction ("Confidential Information").
 * You shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement
 * you entered into with LiveAction.
 */
package com.mrhampson.database;

import java.util.Objects;

/**
 * @author Marshall Hampson
 */
public class RecordBuilder {
    private final TableDefinition tableDefinition;

    private RecordBuilder(TableDefinition tableDefinition) {
        Objects.requireNonNull(tableDefinition);
        this.tableDefinition = tableDefinition;
    }

    public Record.Builder newRecord() {
        return new Record.Builder(tableDefinition);
    }

    public static RecordBuilder forTable(TableDefinition tableDefinition) {
        return new RecordBuilder(tableDefinition);
    }
}

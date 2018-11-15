/*
 * Record.java
 * Created on Nov 15, 2018, 11:41 AM
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

import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author Marshall Hampson
 */
public class Record {
    private final int recordBytes;
    private final Map<String, ColumnValue<?>> columnValues;
    
    private Record(Builder builder) {
        this.recordBytes = builder.recordBytes;
        this.columnValues = Collections.unmodifiableMap(builder.values);
    }
    
    public Map<String, ColumnValue<?>> getColumnValues() {
        return this.columnValues;
    }

    public int getRecordBytes() {
        return recordBytes;
    }
    
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(recordBytes);
        for (ColumnValue<?> value : columnValues.values()) {
            buffer.put(value.toBytes());
        }
        return buffer.array();
    }

    public static final class Builder {
        private final int recordBytes;
        private final TableDefinition tableDefinition;
        private final Map<String, ColumnValue<?>> values;
        
        public Builder(TableDefinition tableDefinition) {
            Objects.requireNonNull(tableDefinition);
            this.tableDefinition = tableDefinition;
            this.values = new LinkedHashMap<>();
            for (ColumnDefinition columnDefinition : this.tableDefinition.getColumns()) {
                this.values.put(columnDefinition.getColumnName(), ColumnValue.fromColumnDefinition(columnDefinition));
            }
            this.recordBytes = tableDefinition.getRowSize();
        }
       
        public Builder setColumnValue(String columnName, Object value) {
            Objects.requireNonNull(columnName);
            ColumnValue<?> columnValue = this.values.get(columnName);
            if (columnValue == null) {
                throw new IllegalArgumentException("Column not defined");
            }
            columnValue.setValue(value);
            return this;
        }
        
        public Record build() {
            return new Record(this);
        }
    }
}

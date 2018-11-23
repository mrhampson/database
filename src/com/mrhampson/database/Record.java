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
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Record[ ");
        for (ColumnValue<?> columnValue : columnValues.values()) {
            sb.append(columnValue.getColumnDefinition().getColumnName());
            sb.append('=');
            sb.append(columnValue.getValue() != null ? columnValue.getValue().toString() : "null");
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append("]");
        return sb.toString();
    }
    
    public static Record fromBytes(TableDefinition tableDefinition, ByteBuffer byteBuffer) {
        Objects.requireNonNull(byteBuffer);
        byteBuffer.rewind();
        Builder builder = new Builder(tableDefinition);
        for (ColumnDefinition definition : tableDefinition.getColumns()) {
            StorageDataType storageDataType = definition.getStorageType();
            switch (storageDataType) {
            case INTEGER:
                builder.setColumnValue(definition.getColumnName(), byteBuffer.getInt());
                break;
            case VARCHAR:
                byte[] stringBytes = new byte[definition.getFieldLength()];
                byteBuffer.get(stringBytes);
                String string = new String(stringBytes, Constants.CHARSET);
                builder.setColumnValue(definition.getColumnName(), string.trim());
            }
        }
        return builder.build();
    }

    static final class Builder {
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

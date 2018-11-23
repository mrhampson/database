package com.mrhampson.database;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author Marshall Hampson
 */
public class TableDefinition {
    static final int MAX_NAME_LENGTH = 256;
    
    private final String tableName;
    private final List<ColumnDefinition> columns;
    private final int rowSizeBytes;

    public TableDefinition(String tableName, List<ColumnDefinition> columns) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(columns);
        if (tableName.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException("Name too long");
        }
        this.tableName = tableName;
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        this.rowSizeBytes = this.columns.stream().mapToInt(ColumnDefinition::getFieldLength).sum();
    }

    public String getTableName() {
        return tableName;
    }

    public List<ColumnDefinition> getColumns() {
        return columns;
    }
    
    public int getRowSize() {
        return this.rowSizeBytes;
    }
    
    public byte[] toBytes() {
        ByteBuffer output = ByteBuffer.allocate(calculateByteLength());
        output.put(tableName.getBytes(Constants.CHARSET));
        output.position(MAX_NAME_LENGTH);
        for (ColumnDefinition columnDefinition : columns) {
            output.put(columnDefinition.toBytes());
        }
        return output.array();
    }
    
    public int calculateByteLength() {
        return MAX_NAME_LENGTH + columns.size() * ColumnDefinition.NUM_BYTES;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableDefinition that = (TableDefinition) o;
        return Objects.equals(tableName, that.tableName) &&
                Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, columns);
    }
}

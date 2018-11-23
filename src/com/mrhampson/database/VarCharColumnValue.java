package com.mrhampson.database;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author Marshall Hampson
 */
public class VarCharColumnValue implements ColumnValue<String> {
    private static final Class<String> CLASS = String.class;
    private final ColumnDefinition columnDefinition;
    private String value = null;
        
    VarCharColumnValue(ColumnDefinition columnDefinition) {
        Objects.requireNonNull(columnDefinition);
        this.columnDefinition = columnDefinition;
    }

    @Override
    public ColumnDefinition getColumnDefinition() {
        return this.columnDefinition;
    }

    @Override
    public void setValue(Object value) {
        if (value != null && !CLASS.equals(value.getClass())) {
            throw new IllegalArgumentException("Wrong type for column");
        }
        String stringValue = (String)value;
        if (stringValue != null && stringValue.length() > columnDefinition.getFieldLength()) {
            throw new IllegalArgumentException("Length out of range");
        }
        this.value = stringValue;
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public byte[] toBytes() {
        if (value == null) {
            return ByteBuffer.allocate(this.columnDefinition.getFieldLength()).array();
        }
        else {
            return ByteBuffer.allocate(this.getColumnDefinition().getFieldLength())
                .put(value.getBytes(Constants.CHARSET))
                .array();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VarCharColumnValue that = (VarCharColumnValue) o;
        return Objects.equals(columnDefinition, that.columnDefinition) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnDefinition, value);
    }
}

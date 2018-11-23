package com.mrhampson.database;


import java.util.Objects;

/**
 * @author Marshall Hampson
 */
public interface ColumnValue<T> {
    ColumnDefinition getColumnDefinition();
    void setValue(Object value);
    T getValue();
    byte[] toBytes();
    
    @SuppressWarnings("unchecked")
    static <T> ColumnValue<T> fromColumnDefinition(ColumnDefinition definition) {
        Objects.requireNonNull(definition);
        switch (definition.getStorageType()) {
            case VARCHAR:
               return (ColumnValue<T>)new VarCharColumnValue(definition); 
        }
        throw new IllegalArgumentException("Unhandled data type");
    }
}

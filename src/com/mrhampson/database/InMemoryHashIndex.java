package com.mrhampson.database;

import java.util.*;

/**
 * @author Marshall Hampson
 */
public class InMemoryHashIndex implements Index {
    private final ColumnDefinition columnDefinition;
    private final Map<Object, List<Long>> valueToRecordLocations;
    
    public InMemoryHashIndex(ColumnDefinition columnDefinition) {
        this.columnDefinition = columnDefinition;
        this.valueToRecordLocations = new HashMap<>();
    }

    @Override
    public ColumnDefinition getColumnDefinition() {
        return columnDefinition;
    }

    public void updateIndex(ColumnValue<?> value, long byteLocation) {
        List<Long> records = this.valueToRecordLocations.computeIfAbsent(value.getValue(), k -> new ArrayList<>());
        records.add(byteLocation);
    }
    
    public List<Long> getLocations(ColumnValue<?> value) {
        List<Long> locations = this.valueToRecordLocations.get(value.getValue());
        return locations != null ? locations : Collections.emptyList();
    }
}

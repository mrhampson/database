package com.mrhampson.database;

import java.util.List;

/**
 * @author Marshall Hampson
 */
public interface Index {
    ColumnDefinition getColumnDefinition();
    void updateIndex(ColumnValue<?> value, long byteLocation);
    List<Long> getLocations(ColumnValue<?> value);
}

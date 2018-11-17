/*
 * InMemoryHashIndex.java
 * Created on Nov 16, 2018, 4:23 PM
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

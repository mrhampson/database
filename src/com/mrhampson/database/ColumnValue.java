/*
 * ColumnValue.java
 * Created on Nov 11, 2018, 12:04 AM
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

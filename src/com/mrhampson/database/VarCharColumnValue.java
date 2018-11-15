/*
 * VarCharColumnValue.java
 * Created on Nov 11, 2018, 12:06 AM
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
}

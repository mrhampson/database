/*
 * ColumnDefinition.java
 * Created on Nov 10, 2018, 6:32 PM
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
public class ColumnDefinition {
    static final int MAX_NAME_LENGTH = 256;
    // Number of bytes in a column def on disk name + fieldLength + storageType
    static final int NUM_BYTES = MAX_NAME_LENGTH + 4 + 1;
    
    private final StorageDataType storageType;
    private final int fieldLength;
    private final String columnName;

    public ColumnDefinition(StorageDataType storageType, int fieldLength, String columnName) {
        Objects.requireNonNull(storageType);
        Objects.requireNonNull(columnName);
        if (columnName.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException("Name too long");
        }
        this.storageType = storageType;
        this.fieldLength = fieldLength;
        this.columnName = columnName;
    }
    
    public StorageDataType getStorageType() {
        return storageType;
    }

    public int getFieldLength() {
        return fieldLength;
    }

    public String getColumnName() {
        return columnName;
    }
    
    public byte[] toBytes() {
        ByteBuffer output = ByteBuffer.allocate(NUM_BYTES);
        // Data type
        int dataTypeOrdinal = storageType.ordinal();
        if (dataTypeOrdinal > 256) {
            throw new IllegalStateException("Data type is too large. Too many data types defined");
        }
        output.put((byte)dataTypeOrdinal);
        // Field Length
        output.putInt(fieldLength);
        // Name
        byte[] nameBytes = columnName.getBytes(Constants.CHARSET);
        output.put(nameBytes);
        return output.array();
    }
    
    public static ColumnDefinition fromBytes(ByteBuffer bytes) {
        Objects.requireNonNull(bytes);
        if (bytes.limit() != ColumnDefinition.NUM_BYTES) {
            throw new IllegalArgumentException("Wrong number of bytes");
        }
        bytes.rewind();
        int dataTypeOrdinal = bytes.get();
        StorageDataType dataType = StorageDataType.fromOrdinal(dataTypeOrdinal);
        int fieldLength = bytes.getInt();
        String columnName = ByteBufferUtils.fromASCIIBytes(bytes);
        return new ColumnDefinition(dataType, fieldLength, columnName.trim());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnDefinition that = (ColumnDefinition) o;
        return fieldLength == that.fieldLength &&
                storageType == that.storageType &&
                Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storageType, fieldLength, columnName);
    }
}

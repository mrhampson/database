package com.mrhampson.database;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * NOTE: Cannot define more than 256 datatypes
 * @author Marshall Hampson
 */
public enum StorageDataType {
    INTEGER,
    VARCHAR;
    
    private static final StorageDataType[] VALUES;
    private static final Map<StorageDataType, Class<?>> STORAGE_TO_JAVA_TYPE;
    
    static {
        VALUES = StorageDataType.values();
        Map<StorageDataType, Class<?>> storageToJavaType = new HashMap<>();
        storageToJavaType.put(INTEGER, Integer.class);
        storageToJavaType.put(VARCHAR, String.class);
        STORAGE_TO_JAVA_TYPE = Collections.unmodifiableMap(storageToJavaType);
    }
    
    public static StorageDataType fromOrdinal(int ordinal) {
        if (ordinal < 0 || ordinal >= VALUES.length) {
            throw new IllegalArgumentException("Ordinal out of bounds");
        }
        return VALUES[ordinal];
    }
    
    public Class<?> getJavaType() {
        return STORAGE_TO_JAVA_TYPE.get(this);
    }
}

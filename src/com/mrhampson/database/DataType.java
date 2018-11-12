/*
 * DataType.java
 * Created on Nov 10, 2018, 6:31 PM
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

/**
 * 
 * NOTE: Cannot define more than 256 datatypes
 * @author Marshall Hampson
 */
public enum DataType {
    INTEGER,
    VARCHAR;
    
    private static final DataType[] VALUES;
    
    static {
        VALUES = DataType.values();
    }
    
    public static DataType fromOrdinal(int ordinal) {
        if (ordinal < 0 || ordinal >= VALUES.length) {
            throw new IllegalArgumentException("Ordinal out of bounds");
        }
        return VALUES[ordinal];
    }
}

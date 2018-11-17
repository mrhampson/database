/*
 * Index.java
 * Created on Nov 16, 2018, 4:28 PM
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

import java.util.List;

/**
 * @author Marshall Hampson
 */
public interface Index {
    ColumnDefinition getColumnDefinition();
    void updateIndex(ColumnValue<?> value, long byteLocation);
    List<Long> getLocations(ColumnValue<?> value);
}

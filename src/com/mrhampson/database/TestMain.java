/*
 * TestMain.java
 * Created on Nov 10, 2018, 7:24 PM
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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.Future;

/**
 * @author Marshall Hampson
 */
public class TestMain {
    public static void main(String[] args) {
        ColumnDefinition nameColumn = new ColumnDefinition(StorageDataType.VARCHAR, 100, "NAME");
        ColumnDefinition cityColumn = new ColumnDefinition(StorageDataType.VARCHAR, 100, "CITY");
        
        TableDefinition tableDefinition = new TableDefinition(
            "PEOPLE",
            Arrays.asList(nameColumn, cityColumn)
        );
        Path dbFilePath = Paths.get("/Users/marshall/Desktop/custom-db.dat");
        try { 
            TableStorageManager dbTableStorageManager = new TableStorageManager(tableDefinition, dbFilePath);
            
            Record newRecord = new Record.Builder(tableDefinition)
                    .setColumnValue("NAME", "Marshall")
                    .setColumnValue("CITY", "Concord")
                    .build();
            dbTableStorageManager.createIndex(new InMemoryHashIndex(nameColumn));
            dbTableStorageManager.storeRecord(newRecord);
                    
            ColumnValue<?> valueToFind = new VarCharColumnValue(nameColumn);
            valueToFind.setValue("Marshall");
            Future<Record> foundRecordFuture = dbTableStorageManager.findFirstMatch(valueToFind);
            assert foundRecordFuture.get().getColumnValues().values().contains(valueToFind);
            dbTableStorageManager.shutdown();
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}

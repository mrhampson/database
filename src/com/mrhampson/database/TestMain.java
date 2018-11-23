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

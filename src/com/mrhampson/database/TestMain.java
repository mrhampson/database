package com.mrhampson.database;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * @author Marshall Hampson
 */
public class TestMain {
    public static void main(String[] args) {
        ColumnDefinition nameColumn = new ColumnDefinition(StorageDataType.VARCHAR, 100, "NAME");
        ColumnDefinition ageColumn = new ColumnDefinition(StorageDataType.VARCHAR, 100, "CITY");
        
        TableDefinition tableDefinition = new TableDefinition(
            "PEOPLE",
            Arrays.asList(nameColumn, ageColumn)
        );
        Path dbFilePath = Paths.get("/Users/marshall/Desktop/custom-db.dat");
        
        TableStorageManager dbTableStorageManager = new TableStorageManager(dbFilePath);
        try {
            dbTableStorageManager.create(tableDefinition);
            dbTableStorageManager.load();
            
            Record newRecord = new Record.Builder(tableDefinition)
                    .setColumnValue("NAME", "Marshall")
                    .setColumnValue("CITY", "Concord")
                    .build();
            dbTableStorageManager.createIndex(new InMemoryHashIndex(nameColumn));
            dbTableStorageManager.storeRecord(newRecord);
                    
            ColumnValue<?> valueToFind = new VarCharColumnValue(nameColumn);
            valueToFind.setValue("Marshall");
            Record foundRecord = dbTableStorageManager.findFirstMatch(valueToFind);
            dbTableStorageManager.shutdown();
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}

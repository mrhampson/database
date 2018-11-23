package com.mrhampson.database;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;

/**
 * @author Marshall Hampson
 */
public class TestMain {
    private static final Random RANDOM = new Random();
    
    public static void main(String[] args) {
        ColumnDefinition nameColumn = new ColumnDefinition(StorageDataType.VARCHAR, 100, "NAME");
        ColumnDefinition cityColumn = new ColumnDefinition(StorageDataType.VARCHAR, 100, "CITY");
        
        TableDefinition tableDefinition = new TableDefinition(
            "PEOPLE",
            Arrays.asList(nameColumn, cityColumn)
        );
        Path dbFilePath = Paths.get("/Users/marshall/Desktop/custom-db.dat");
        try {
            RecordBuilder recordBuilder = RecordBuilder.forTable(tableDefinition);
            TableStorageManager dbTableStorageManager = new TableStorageManager(tableDefinition, dbFilePath);
            dbTableStorageManager.createIndex(new InMemoryHashIndex(nameColumn));
            List<Record> records = new ArrayList<>(1_000_000);
            for (int i = 0; i < 1_000_000; i++) {
                Record record = recordBuilder.newRecord()
                    .setColumnValue("NAME", randomASCIIString(50))
                    .setColumnValue("CITY", randomASCIIString(50))
                    .build();
                records.add(record);
            }
            records.add(recordBuilder.newRecord()
                    .setColumnValue("NAME", "Marshall")
                    .setColumnValue("CITY", "Concord")
                    .build());
            dbTableStorageManager.bulkStoreRecords(records);
            
            ColumnValue<?> valueToFind = new VarCharColumnValue(nameColumn);
            valueToFind.setValue("Marshall");
            Future<Record> foundRecordFuture = dbTableStorageManager.findFirstMatch(valueToFind);
            Record found = foundRecordFuture.get();
            assert found.getColumnValues().values().contains(valueToFind);
            System.out.println(found);
            dbTableStorageManager.shutdown();
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static String randomASCIIString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int charCode = RANDOM.nextInt(93) + 33;
            sb.append((char)charCode);
        }
        return sb.toString();
    }
}

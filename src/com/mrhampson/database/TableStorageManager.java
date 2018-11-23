package com.mrhampson.database;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * @author Marshall Hampson
 */
public class TableStorageManager {
    private static final int HEADER_SEPERATOR_VALUE = 0xFFFFFFFF;
    
    private TableDefinition tableDefinition;
    private final Map<String, Index> indexes = new HashMap<>();
    private final Path tableFilePath;
    private FileChannel fileChannel = null;
    private long tailByte = -1;
    
    public TableStorageManager(Path tableFilePath) {
        this.tableFilePath = tableFilePath;
    }

    public void create(TableDefinition tableDefinition) throws IOException {
       tableFilePath.toFile().delete();
       writeHeader(tableDefinition);
    }
    
    public void createIndex(Index index) throws IOException {
        indexes.put(index.getColumnDefinition().getColumnName(), index);
        long startPos = calculateDataStartByte(this.tableDefinition);
        fileChannel.position(startPos);
        ByteBuffer rowBuf = ByteBuffer.allocate(tableDefinition.getRowSize());
        while (fileChannel.read(rowBuf) > 0) {
            Record record = Record.fromBytes(tableDefinition, rowBuf);
            rowBuf.clear();
            ColumnValue<?> valueForIndex = record.getColumnValues().get(index.getColumnDefinition().getColumnName()); 
            if (valueForIndex != null) {
                index.updateIndex(valueForIndex, fileChannel.position() - record.getRecordBytes());
            }
        }
    }
    
    
    public void storeRecord(Record record) throws IOException {
        fileChannel.position(tailByte);
        // Update indexes
        for (ColumnValue<?> value : record.getColumnValues().values()) {
            Index index = this.indexes.get(value.getColumnDefinition().getColumnName());
            if (index != null) {
                index.updateIndex(value, tailByte);
            }
        }
        
        fileChannel.write(ByteBuffer.wrap(record.toBytes()));
        tailByte = fileChannel.position();
    }
    
    public Record findFirstMatch(ColumnValue<?> valueToMatch) throws IOException {
        ByteBuffer rowBuf = ByteBuffer.allocate(tableDefinition.getRowSize());
        // Check if we can use an index
        Index index = indexes.get(valueToMatch.getColumnDefinition().getColumnName());
        if (index != null) {
            List<Long> locations = index.getLocations(valueToMatch);    
            fileChannel.position(locations.get(0));
            fileChannel.read(rowBuf);
            return Record.fromBytes(tableDefinition, rowBuf);
        }
        
        long startPos = calculateDataStartByte(this.tableDefinition);
        fileChannel.position(startPos);
        while (fileChannel.read(rowBuf) > 0) {
            Record record = Record.fromBytes(tableDefinition, rowBuf);
            rowBuf.clear();
            ColumnValue<?> value = record.getColumnValues().get(valueToMatch.getColumnDefinition().getColumnName());
            if (value.equals(valueToMatch)) {
                return record;
            }
        }
        return null;
    }
    
    public void load() throws IOException {
        fileChannel = FileChannel.open(tableFilePath, READ, WRITE);
        this.tableDefinition = readHeader();
        tailByte = fileChannel.position();
    }
    
    private TableDefinition readHeader() throws IOException {
        ByteBuffer tableNameBuf = ByteBuffer.allocate(TableDefinition.MAX_NAME_LENGTH);
        fileChannel.read(tableNameBuf);
        String tableName = new String(tableNameBuf.array(), Constants.CHARSET).trim();

        List<ColumnDefinition> columns = new ArrayList<>();
        ByteBuffer seperatorBuf = ByteBuffer.allocate(4);
        ByteBuffer columnDefBuf = ByteBuffer.allocate(ColumnDefinition.NUM_BYTES);
        while (true) {
            long intialPos = fileChannel.position();
            int bytesRead = fileChannel.read(seperatorBuf);
            if (bytesRead == -1) {
                throw new IllegalArgumentException("Unexpected end of file");
            }
            seperatorBuf.rewind();
            int firstByteValue = seperatorBuf.getInt();
            if (firstByteValue == HEADER_SEPERATOR_VALUE) {
                break;
            }
            fileChannel.position(intialPos);
            fileChannel.read(columnDefBuf);
            columns.add(ColumnDefinition.fromBytes(columnDefBuf));
            
            seperatorBuf.clear();
            columnDefBuf.clear();
        }
        return new TableDefinition(tableName, columns);
    }
    
    private void writeHeader(TableDefinition tableDefinition) throws IOException {
        try(
            FileChannel fileChannel = FileChannel.open(tableFilePath, READ, WRITE, CREATE);
        ) {
            fileChannel.write(ByteBuffer.wrap(tableDefinition.toBytes()));
            ByteBuffer separator = ByteBuffer.allocate(4).putInt(HEADER_SEPERATOR_VALUE);
            separator.rewind();
            fileChannel.write(separator);
        }
    }
    
    public void shutdown() throws IOException {
        if (fileChannel != null) {
            fileChannel.close();
        }
    }
    
    private static long calculateDataStartByte(TableDefinition tableDefinition) {
        Objects.requireNonNull(tableDefinition);
        return tableDefinition.calculateByteLength() + 4;
    }
}

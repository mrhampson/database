/*
 * TableStorageManager.java
 * Created on Nov 10, 2018, 6:45 PM
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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * @author Marshall Hampson
 */
public class TableStorageManager {
    private static final int HEADER_SEPERATOR_VALUE = 0xFFFFFFFF;
    
    private volatile TableDefinition tableDefinition;
    private final Path tableFilePath;
    private volatile FileChannel fileChannel = null;
    
    public TableStorageManager(Path tableFilePath) {
        this.tableFilePath = tableFilePath;
    }

    public void create(TableDefinition tableDefinition) throws IOException {
       tableFilePath.toFile().delete();
       writeHeader(tableDefinition);
    }
    
    public void load() throws IOException {
        fileChannel = FileChannel.open(tableFilePath, READ, WRITE);
        TableDefinition tableDefinition = readHeader();
        this.tableDefinition = tableDefinition;
    }
    
    private TableDefinition readHeader() throws IOException {
        try(
            FileChannel fileChannel = FileChannel.open(tableFilePath, READ, WRITE, CREATE);
        ) {
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
                    break;
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
    }
    
    private void writeHeader(TableDefinition tableDefinition) throws IOException {
        try(
            FileChannel fileChannel = FileChannel.open(tableFilePath, READ, WRITE, CREATE);
        ) {
            fileChannel.write(ByteBuffer.wrap(tableDefinition.toBytes()));
            fileChannel.write(ByteBuffer.allocate(4).putInt(HEADER_SEPERATOR_VALUE));
        }
    }
    
    public void shutdown() throws IOException {
        if (fileChannel != null) {
            fileChannel.close();
        }
    }
}

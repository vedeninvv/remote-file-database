package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class TableImpl implements Table {
    private String tableName;
    private Path pathToTable;
    private TableIndex tableIndex;
    private Segment curSegment;

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        Path pathToTable = Paths.get(pathToDatabaseRoot.toString(), tableName);
        try {
            Files.createDirectory(pathToTable);
        } catch (IOException e) {
            throw new DatabaseException("IO exception when creating table " + tableName + " with path " + pathToTable.toString(), e);
        }
        return new TableImpl(tableName, pathToTable, tableIndex);
    }

    private TableImpl(String tableName, Path pathToTable, TableIndex tableIndex) {
        this.tableName = tableName;
        this.pathToTable = pathToTable;
        this.tableIndex = tableIndex;
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        if (objectKey == null) throw new DatabaseException("Null key");
        if (curSegment == null)
            curSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), pathToTable);
        try {
            boolean result = curSegment.write(objectKey, objectValue);
            if (!result) {
                curSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), pathToTable);
                curSegment.write(objectKey, objectValue);
            }
        } catch (IOException e) {
            throw new DatabaseException("IOException when writing to segment " + curSegment.getName() + " by key " + objectKey, e);
        }
        tableIndex.onIndexedEntityUpdated(objectKey, curSegment);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        if (objectKey == null) throw new DatabaseException("Null key");
        var segment = tableIndex.searchForKey(objectKey);
        Optional<byte[]> objectValue = Optional.empty();
        try {
            if (segment.isPresent()) objectValue = segment.get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException("IOException when reading segment " + curSegment.getName() + " by key " + objectKey, e);
        }
        return objectValue;
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        if (objectKey == null) throw new DatabaseException("Null key");
        var segment = tableIndex.searchForKey(objectKey);
        if (segment.isEmpty()) throw new DatabaseException("Segment by key " + objectKey + " not found");
        try {
            boolean result = segment.get().delete(objectKey);
            if (!result)
                throw new DatabaseException("Segment " + curSegment.getName() + " is readOnly. Can not delete");
        } catch (IOException e) {
            throw new DatabaseException("IOException when deleting object in segment " + curSegment.getName() + " by key " + objectKey, e);
        }
        tableIndex.onIndexedEntityUpdated(objectKey, null);
    }
}

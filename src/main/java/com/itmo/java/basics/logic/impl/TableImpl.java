package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class TableImpl implements Table {
    private String tableName;
    private Path pathToTable;
    private TableIndex tableIndex;
    private Segment curSegment;

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        Path pathToTable = Path.of(pathToDatabaseRoot.toString() + "/" + tableName);
        try {
            Files.createDirectory(pathToTable);
        } catch (IOException e) {
            throw new DatabaseException("Can not create a directory", e);
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
        if (curSegment == null || curSegment.isReadOnly())
            curSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), pathToTable);
        try{
            boolean result = curSegment.write(objectKey, objectValue);
            if (!result) throw new DatabaseException("False result of writing to Segment");
        }catch (IOException e){
            throw new DatabaseException("Can not write to segment", e);
        }
        tableIndex.onIndexedEntityUpdated(objectKey, curSegment);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        var segment = tableIndex.searchForKey(objectKey);
        Optional<byte[]> objectValue = Optional.empty();
        try {
            if (segment.isPresent()) objectValue = segment.get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException("Can not read segment", e);
        }
        return objectValue;
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        var segment = tableIndex.searchForKey(objectKey);
        try {
            if (segment.isPresent()) segment.get().delete(objectKey);
        } catch (IOException e) {
            throw new DatabaseException("Can not read segment", e);
        }
        tableIndex.onIndexedEntityUpdated(objectKey, null);
    }
}

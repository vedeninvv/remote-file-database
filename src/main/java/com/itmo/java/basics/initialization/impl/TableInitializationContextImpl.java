package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;

import java.nio.file.Path;
import java.nio.file.Paths;

public class TableInitializationContextImpl implements TableInitializationContext {
    private String tableName;
    private Path databasePath;
    private TableIndex tableIndex;
    private Segment curSegment;

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        this.tableName = tableName;
        this.databasePath = databasePath;
        this.tableIndex = tableIndex;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Path getTablePath() {
        return Paths.get(String.valueOf(databasePath), tableName);
    }

    @Override
    public TableIndex getTableIndex() {
        return tableIndex;
    }

    @Override
    public Segment getCurrentSegment() {
        return curSegment;
    }

    @Override
    public void updateCurrentSegment(Segment segment) {
        this.curSegment = segment;
    }
}
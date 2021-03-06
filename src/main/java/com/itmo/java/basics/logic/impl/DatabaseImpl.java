package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private String dbName;
    private Path databasePath;
    private Map<String, Table> tables = new HashMap<>();

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if (dbName == null || databaseRoot == null) {
            throw new DatabaseException("dbName is null or databaseRoot is null");
        }
        Path PathToDatabase = Paths.get(databaseRoot.toString(), dbName);
        try {
            Files.createDirectory(PathToDatabase);
        } catch (IOException e) {
            throw new DatabaseException("IO exception when creating database " + dbName + " with path " + PathToDatabase.toString(), e);
        }
        return new DatabaseImpl(dbName, PathToDatabase);
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return new DatabaseImpl(context);
    }

    private DatabaseImpl(DatabaseInitializationContext context) {
        this(context.getDbName(), context.getDatabasePath());
        this.tables = context.getTables();
    }

    private DatabaseImpl(String dbName, Path databaseRoot) {
        this.dbName = dbName;
        this.databasePath = databaseRoot;
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tableName == null) {
            throw new DatabaseException("tableName is null");
        }
        if (tables.containsKey(tableName)) {
            throw new DatabaseException("Table with name " + tableName + " already exists");
        }
        Table newTable = TableImpl.create(tableName, databasePath, new TableIndex());
        tables.put(tableName, newTable);
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        Table table = tables.get(tableName);
        if (table == null) {
            throw new DatabaseException("Table with name " + tableName + " not found");
        }
        table.write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        Table table = tables.get(tableName);
        if (table == null) {
            throw new DatabaseException("Table with name " + tableName + " not found");
        }
        return table.read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        Table table = tables.get(tableName);
        if (table == null) {
            throw new DatabaseException("Table with name " + tableName + " not found");
        }
        table.delete(objectKey);
    }
}

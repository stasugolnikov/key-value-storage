package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private final Path databasePath;
    private final Map<String, Table> tables;

    private DatabaseImpl(Path databasePath) {
        this.databasePath = databasePath;
        this.tables = new HashMap<>();
    }
    private DatabaseImpl(Path databasePath, Map<String, Table> tables) {
        this.databasePath = databasePath;
        this.tables = tables;
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if (dbName == null) {
            throw new DatabaseException("Database name is null");
        }
        Path databasePath;
        try {
            databasePath = Files.createDirectory(Path.of(databaseRoot.toString() + File.separator + dbName));
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO exception when creating database %s to path %s",
                    dbName, databaseRoot.toString()), e);
        }
        return new DatabaseImpl(databasePath);
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return new DatabaseImpl(context.getDatabasePath(), context.getTables());
    }

    @Override
    public String getName() {
        return databasePath.getFileName().toString();
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tables.containsKey(tableName)) {
            throw new DatabaseException(String.format("Table with name %s already exists", tableName));
        }
        tables.put(tableName, TableImpl.create(tableName, databasePath, new TableIndex()));
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException(String.format("Nonexistent table with name %s", tableName));
        }
        tables.get(tableName).write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException(String.format("Nonexistent table with name %s", tableName));
        }
        return tables.get(tableName).read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException(String.format("Nonexistent table with name %s", tableName));
        }
        tables.get(tableName).delete(objectKey);
    }
}

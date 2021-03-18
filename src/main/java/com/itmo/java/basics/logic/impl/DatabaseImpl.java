package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

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

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        Path databasePath;
        try {
            databasePath = Files.createDirectory(Path.of(databaseRoot.toString() + dbName));
        } catch (IOException exception) {
            throw new DatabaseException(exception.getMessage());
        }
        return new DatabaseImpl(databasePath);
    }

    @Override
    public String getName() {
        return databasePath.getFileName().toString();
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tables.containsKey(tableName)) return;
        tables.put(tableName, TableImpl.create(tableName, databasePath, new TableIndex()));
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        if (!tables.containsKey(tableName)) throw new DatabaseException("Nonexistent table");
        tables.get(tableName).write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        if (!tables.containsKey(tableName)) throw new DatabaseException("Nonexistent table");
        return tables.get(tableName).read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        if (!tables.containsKey(tableName)) throw new DatabaseException("Nonexistent table");
        tables.get(tableName).delete(objectKey);
    }
}

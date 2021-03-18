package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {
    private final Path databasePath;
    private final Map<String, Table> tables;

    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        databasePath = Path.of(databaseRoot.toString() + File.separator + dbName);
        tables = new HashMap<>();
    }

    @Override
    public String getDbName() {
        return databasePath.getFileName().toString();
    }

    @Override
    public Path getDatabasePath() {
        return databasePath;
    }

    @Override
    public Map<String, Table> getTables() {
        return tables;
    }

    @Override
    public void addTable(Table table) {
        tables.put(table.getName(), table);
    }
}

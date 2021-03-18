package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.logic.Database;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {
    private final DatabaseConfig config;
    private final Map<String, Database> databases = new HashMap<>();

    public ExecutionEnvironmentImpl() {
        this.config = new DatabaseConfig(DatabaseConfig.DEFAULT_WORKING_PATH);
    }

    public ExecutionEnvironmentImpl(DatabaseConfig config) {
        this.config = config;
    }

    @Override
    public Optional<Database> getDatabase(String name) {
        if (databases.containsKey(name)) {
            return Optional.of(databases.get(name));
        }
        return Optional.empty();
    }

    @Override
    public void addDatabase(Database db) {
        if (databases.containsKey(db.getName())) {
            return;
        }
        databases.put(db.getName(), db);
    }

    @Override
    public Path getWorkingPath() {
        return Path.of(config.getWorkingPath());
    }
}

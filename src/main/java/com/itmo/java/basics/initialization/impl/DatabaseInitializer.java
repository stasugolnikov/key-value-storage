package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.nio.file.Files;

public class DatabaseInitializer implements Initializer {
    private final TableInitializer tableInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *                           или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        if (!Files.exists(initialContext.currentDbContext().getDatabasePath())) {
            throw new DatabaseException(String.format("Database with path %s does not exist",
                    initialContext.currentDbContext().getDatabasePath().toString()));
        }
        File[] tablesDirectories = initialContext.currentDbContext().getDatabasePath().toFile().listFiles();
        if (tablesDirectories == null) {
            throw new DatabaseException(String.format("Error when getting tables directories from %s",
                    initialContext.currentDbContext().getDatabasePath().toString()));
        }
        for (File tableDirectory : tablesDirectories) {
            if (!Files.isDirectory(tableDirectory.toPath())) {
                continue;
            }
            tableInitializer.perform(InitializationContextImpl
                    .builder()
                    .executionEnvironment(initialContext.executionEnvironment())
                    .currentDatabaseContext(initialContext.currentDbContext())
                    .currentTableContext(new TableInitializationContextImpl(tableDirectory.getName(),
                            initialContext.currentDbContext().getDatabasePath(),
                            new TableIndex()))
                    .build());
        }
        initialContext.executionEnvironment().addDatabase(
                DatabaseImpl.initializeFromContext(initialContext.currentDbContext()));
    }
}

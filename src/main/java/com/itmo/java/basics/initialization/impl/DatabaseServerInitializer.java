package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class DatabaseServerInitializer implements Initializer {
    private final DatabaseInitializer databaseInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, начинает их инициализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        if (!Files.exists(context.executionEnvironment().getWorkingPath())) {
            try {
                Files.createDirectory(context.executionEnvironment().getWorkingPath());
                return;
            } catch (IOException e) {
                throw new DatabaseException(String.format("IOException when creating directory %s",
                        context.executionEnvironment().getWorkingPath()), e);
            }
        }
        File[] dbDirectories = context.executionEnvironment().getWorkingPath().toFile().listFiles();
        if (dbDirectories == null) {
            throw new DatabaseException(String.format("Error when getting databases directories from %s",
                    context.executionEnvironment().getWorkingPath().toString()));
        }
        for (File dbDirectory : dbDirectories) {
            if (!Files.isDirectory(dbDirectory.toPath())) {
                continue;
            }
            databaseInitializer.perform(InitializationContextImpl
                    .builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(new DatabaseInitializationContextImpl(
                            dbDirectory.getName(),
                            context.executionEnvironment().getWorkingPath()))
                    .build());
        }
    }
}

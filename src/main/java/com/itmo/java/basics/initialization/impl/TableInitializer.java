package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;

public class TableInitializer implements Initializer {
    private final SegmentInitializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *                           или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        if (!Files.exists(context.currentTableContext().getTablePath())) {
            throw new DatabaseException(String.format("Table with path %s does not exist",
                    context.currentTableContext().getTablePath().toString()));
        }
        File[] segmentsFiles = context.currentTableContext().getTablePath().toFile().listFiles();
        if (segmentsFiles == null) {
            throw new DatabaseException(String.format("Error when getting tables directories from %s",
                    context.currentDbContext().getDatabasePath().toString()));
        }
        Arrays.sort(segmentsFiles, Comparator.comparingLong(
                file -> Long.parseLong(file.getName().substring(context.currentTableContext().getTableName().length() + 1)))
        );
        for (File segmentFile : segmentsFiles) {
            if (Files.isDirectory(segmentFile.toPath())) {
                continue;
            }
            segmentInitializer.perform(InitializationContextImpl
                    .builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(context.currentDbContext())
                    .currentTableContext(context.currentTableContext())
                    .currentSegmentContext(new SegmentInitializationContextImpl(segmentFile.getName(),
                            context.currentTableContext().getTablePath(),
                            0))
                    .build());
        }
        context.currentDbContext().addTable(TableImpl.initializeFromContext(context.currentTableContext()));
    }
}

package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;


public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        if (!Files.exists(context.currentSegmentContext().getSegmentPath())) {
            throw new DatabaseException(String.format("Segment with path %s does not exist",
                    context.currentSegmentContext().getSegmentPath().toString()));
        }
        try (DatabaseInputStream dbInputStream = new DatabaseInputStream(
                new FileInputStream(context.currentSegmentContext().getSegmentPath().toString()))) {
            Optional<DatabaseRecord> dbRecord = dbInputStream.readDbUnit();
            Set<String> keys = new HashSet<>();
            while (dbRecord.isPresent()) {
                String key = new String(dbRecord.get().getKey());
                keys.add(key);
                context.currentSegmentContext().getIndex().onIndexedEntityUpdated(key,
                        new SegmentOffsetInfoImpl(context.currentSegmentContext().getCurrentSize()));
                context = InitializationContextImpl
                        .builder()
                        .executionEnvironment(context.executionEnvironment())
                        .currentDatabaseContext(context.currentDbContext())
                        .currentTableContext(context.currentTableContext())
                        .currentSegmentContext(new SegmentInitializationContextImpl(
                                context.currentSegmentContext().getSegmentName(),
                                context.currentSegmentContext().getSegmentPath(),
                                context.currentSegmentContext().getCurrentSize() + dbRecord.get().size(),
                                context.currentSegmentContext().getIndex()))
                        .build();
                dbRecord = dbInputStream.readDbUnit();
            }
            context.currentTableContext().updateCurrentSegment(
                    SegmentImpl.initializeFromContext(context.currentSegmentContext()));
            for (String key : keys) {
                context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key,
                        context.currentTableContext().getCurrentSegment());
            }
        } catch (IOException e) {
            throw new DatabaseException(String.format("IOException when reading from segment %s",
                    context.currentSegmentContext().getSegmentPath().toString()), e);
        }

    }
}

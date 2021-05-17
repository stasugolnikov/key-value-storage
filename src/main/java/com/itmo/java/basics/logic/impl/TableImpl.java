package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public class TableImpl implements Table {
    private final Path tablePath;
    private final TableIndex tableIndex;
    private Segment currentSegment;

    private TableImpl(Path tablePath, TableIndex tableIndex) {
        this.tablePath = tablePath;
        this.tableIndex = tableIndex;
        this.currentSegment = null;
    }

    private TableImpl(Path tablePath, TableIndex tableIndex, Segment currentSegment) {
        this.tablePath = tablePath;
        this.tableIndex = tableIndex;
        this.currentSegment = currentSegment;
    }

    public static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        if (tableName == null) {
            throw new DatabaseException("Table name is null");
        }
        Path tablePath;
        try {
            tablePath = Files.createDirectory(Path.of(pathToDatabaseRoot.toString() + File.separator + tableName));
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO exception when creating table %s to path %s",
                    tableName, pathToDatabaseRoot), e);
        }
        return new CachingTable(new TableImpl(tablePath, tableIndex), new DatabaseCacheImpl());
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        return new CachingTable(new TableImpl(context.getTablePath(), context.getTableIndex(), context.getCurrentSegment()),
                new DatabaseCacheImpl());
    }

    @Override
    public String getName() {
        return tablePath.getFileName().toString();
    }

    private void updateSegment() throws DatabaseException {
        currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(getName()), tablePath);
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        if (currentSegment == null) {
            updateSegment();
        }
        try {
            if (currentSegment.write(objectKey, objectValue)) {
                tableIndex.onIndexedEntityUpdated(objectKey, currentSegment);
                return;
            }
            updateSegment();
            currentSegment.write(objectKey, objectValue);
            tableIndex.onIndexedEntityUpdated(objectKey, currentSegment);
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO exception when writing key %s in table %s, segment %s",
                    objectKey, getName(), currentSegment.getName()), e);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        Optional<Segment> segment = tableIndex.searchForKey(objectKey);
        if (segment.isEmpty()) {
            return Optional.empty();
        }
        try {
            return segment.get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO exception when reading key %s in table %s, segment %s",
                    objectKey, getName(), currentSegment.getName()), e);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        Optional<Segment> segment = tableIndex.searchForKey(objectKey);
        if (segment.isEmpty()) {
            throw new DatabaseException(String.format("Nonexistent key %s", objectKey));
        }
        try {
            if (currentSegment.delete(objectKey)) {
                tableIndex.onIndexedEntityUpdated(objectKey, currentSegment);
                return;
            }
            updateSegment();
            currentSegment.delete(objectKey);
            tableIndex.onIndexedEntityUpdated(objectKey, currentSegment);
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO exception when deleting key %s in table %s, segment %s",
                    objectKey, getName(), currentSegment.getName()), e);
        }
    }
}

package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class TableImpl implements Table {
    private final Path tablePath;
    private final TableIndex tableIndex;
    private Segment curSegment;

    private TableImpl(Path tablePath, TableIndex tableIndex) {
        this.tablePath = tablePath;
        this.tableIndex = tableIndex;
        this.curSegment = null;
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        if (tableName == null) throw new DatabaseException("Table name is null");
        Path tablePath;
        try {
            tablePath = Files.createDirectory(Path.of(pathToDatabaseRoot.toString() + File.separator + tableName));
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO exception when creating table %s to path %s",
                    tableName, pathToDatabaseRoot.toString()), e);
        }
        return new TableImpl(tablePath, tableIndex);
    }

    @Override
    public String getName() {
        return tablePath.getFileName().toString();
    }

    private void updateSegment() throws DatabaseException {
        curSegment = SegmentImpl.create(SegmentImpl.createSegmentName(getName()), tablePath);
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        if (curSegment == null) updateSegment();
        try {
            if (curSegment.write(objectKey, objectValue)) {
                tableIndex.onIndexedEntityUpdated(objectKey, curSegment);
                return;
            }
            updateSegment();
            curSegment.write(objectKey, objectValue);
            tableIndex.onIndexedEntityUpdated(objectKey, curSegment);
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO exception when writing key %s in table %s, segment %s",
                    objectKey, getName(), curSegment.getName()), e);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        Optional<Segment> segment = tableIndex.searchForKey(objectKey);
        if (segment.isEmpty()) return Optional.empty();
        try {
            return segment.get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO exception when reading key %s in table %s, segment %s",
                    objectKey, getName(), curSegment.getName()), e);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        Optional<Segment> segment = tableIndex.searchForKey(objectKey);
        if (segment.isEmpty()) throw new DatabaseException(String.format("Nonexistent key %s", objectKey));
        try {
            if (curSegment.delete(objectKey)) {
                tableIndex.onIndexedEntityUpdated(objectKey, curSegment);
                return;
            }
            updateSegment();
            curSegment.delete(objectKey);
            tableIndex.onIndexedEntityUpdated(objectKey, curSegment);
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO exception when deleting key %s in table %s, segment %s",
                    objectKey, getName(), curSegment.getName()), e);
        }
    }
}

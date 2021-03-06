package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class SegmentImpl implements Segment {
    private final Path segmentPath;
    private final SegmentIndex segmentIndex;
    private long segmentOffset;

    private SegmentImpl(Path segmentPath) {
        this.segmentPath = segmentPath;
        this.segmentIndex = new SegmentIndex();
        this.segmentOffset = 0;
    }

    private SegmentImpl(Path segmentPath, SegmentIndex segmentIndex, long segmentOffset) {
        this.segmentPath = segmentPath;
        this.segmentIndex = segmentIndex;
        this.segmentOffset = segmentOffset;
    }

    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        Path segmentPath;
        try {
            segmentPath = Files.createFile(Paths.get(tableRootPath.toString() + File.separator + segmentName));
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO exception when creating segment %s to path %s",
                    segmentName, tableRootPath), e);
        }
        return new SegmentImpl(segmentPath);
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        return new SegmentImpl(context.getSegmentPath(), context.getIndex(), context.getCurrentSize());
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return segmentPath.getFileName().toString();
    }

    private void writeDatabaseRecord(WritableDatabaseRecord dbRecord) throws IOException {
        try (var dbOutputStream =
                     new DatabaseOutputStream(new FileOutputStream(segmentPath.toString(), true))) {
            segmentIndex.onIndexedEntityUpdated(new String(dbRecord.getKey()), new SegmentOffsetInfoImpl(segmentOffset));
            segmentOffset += dbOutputStream.write(dbRecord);
        }
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        if (isReadOnly()) {
            return false;
        }
        var dbRecord = new SetDatabaseRecord(objectKey.getBytes(), objectValue);
        writeDatabaseRecord(dbRecord);
        return true;
    }

    private Optional<DatabaseRecord> readDatabaseRecord(long offset) throws IOException {
        try (var dbInputStream = new DatabaseInputStream(new FileInputStream(segmentPath.toString()))) {
            long skipped = dbInputStream.skip(offset);
            if (skipped != offset) {
                throw new IOException(String.format("Error in skipping bytes in file %s", getName()));
            }
            return dbInputStream.readDbUnit();
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        Optional<SegmentOffsetInfo> offset = segmentIndex.searchForKey(objectKey);
        if (offset.isEmpty()) {
            return Optional.empty();
        }
        var dbRecord = readDatabaseRecord(offset.get().getOffset());
        return dbRecord.map(DatabaseRecord::getValue);
    }

    @Override
    public boolean isReadOnly() {
        return segmentPath.toFile().length() >= 100000;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        if (isReadOnly()) {
            return false;
        }
        writeDatabaseRecord(new RemoveDatabaseRecord(objectKey.getBytes()));
        return true;
    }
}

package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;

import java.io.File;
import java.nio.file.Path;

public class TableInitializationContextImpl implements TableInitializationContext {
    private final Path tablePath;
    private final TableIndex tableIndex;
    private Segment currentSegment;

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        this.tablePath = Path.of(databasePath.toString() + File.separator + tableName);
        this.tableIndex = tableIndex;
        this.currentSegment = null;
    }

    @Override
    public String getTableName() {
        return tablePath.getFileName().toString();
    }

    @Override
    public Path getTablePath() {
        return tablePath;
    }

    @Override
    public TableIndex getTableIndex() {
        return tableIndex;
    }

    @Override
    public Segment getCurrentSegment() {
        return currentSegment;
    }

    @Override
    public void updateCurrentSegment(Segment segment) {
        currentSegment = segment;
    }
}

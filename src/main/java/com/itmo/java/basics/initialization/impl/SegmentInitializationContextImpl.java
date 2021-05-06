package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.SegmentInitializationContext;

import java.io.File;
import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {
    private final Path segmentPath;
    private final SegmentIndex segmentIndex;
    private long currentSize;

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, long currentSize, SegmentIndex index) {
        this.segmentPath = segmentPath;
        this.segmentIndex = index;
        this.currentSize = currentSize;
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath, long currentSize) {
        this.segmentPath = Path.of(tablePath.toString() + File.separator + segmentName);
        this.segmentIndex = new SegmentIndex();
        this.currentSize = currentSize;
    }

    @Override
    public String getSegmentName() {
        return segmentPath.getFileName().toString();
    }

    @Override
    public Path getSegmentPath() {
        return segmentPath;
    }

    @Override
    public SegmentIndex getIndex() {
        return segmentIndex;
    }

    @Override
    public long getCurrentSize() {
        return currentSize;
    }
}

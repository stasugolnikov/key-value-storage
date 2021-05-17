package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCacheImpl implements DatabaseCache {
    private static final int CAPACITY = 5_000;

    private final Map<String, byte[]> dbCacheMap;

    DatabaseCacheImpl() {
        this.dbCacheMap = new LinkedHashMap<>(CAPACITY, 1f, true) {
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > CAPACITY;
            }
        };
    }

    @Override
    public byte[] get(String key) {
        return dbCacheMap.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        dbCacheMap.put(key, value);
    }

    @Override
    public void delete(String key) {
        dbCacheMap.remove(key);
    }
}

package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCacheImpl implements DatabaseCache {
    private final Map<String, byte[]> dbCacheMap;

    DatabaseCacheImpl(int capacity) {
        this.dbCacheMap = new LinkedHashMap<>(capacity, 1f, true) {
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > capacity;
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

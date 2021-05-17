package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.DatabaseCache;
import com.itmo.java.basics.logic.Table;

import java.util.Optional;

/**
 * Декоратор для таблицы. Кэширует данные
 */
public class CachingTable implements Table {
    private final Table table;
    private final DatabaseCache databaseCache;

    CachingTable(Table table, DatabaseCache databaseCache) {
        this.table = table;
        this.databaseCache = databaseCache;
    }

    @Override
    public String getName() {
        return table.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        databaseCache.set(objectKey, objectValue);
        table.write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        if (databaseCache.get(objectKey) != null) {
            return Optional.of(databaseCache.get(objectKey));
        }
        return table.read(objectKey);
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        if (databaseCache.get(objectKey) != null) {
            databaseCache.delete(objectKey);
        }
        table.delete(objectKey);
    }
}

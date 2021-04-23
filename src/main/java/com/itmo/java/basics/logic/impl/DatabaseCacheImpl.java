package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCacheImpl extends LinkedHashMap<String, byte[]> implements DatabaseCache {
    private static final int CAPACITY = 5000;

    public DatabaseCacheImpl() {
        super(CAPACITY, 1f, true);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
        return size() > CAPACITY;
    }

    @Override
    public byte[] get(String key) {
        return super.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        super.put(key, value);
    }

    @Override
    public void delete(String key) {
        super.remove(key);
    }
}

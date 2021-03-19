package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

public class SetDatabaseRecord implements WritableDatabaseRecord {
    private static final int sizeOfKeySize = 4;
    private static final int sizeOfValueSize = 4;
    private int keySize, valueSize;
    private byte[] key, value;

    public SetDatabaseRecord(int keySize, byte[] key, int valueSize, byte[] value){
        this.keySize = keySize;
        this.key = key;
        this.valueSize = valueSize;
        this.value = value;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public long size() {
        return sizeOfKeySize + keySize + sizeOfValueSize + valueSize;
    }

    @Override
    public boolean isValuePresented() {
        return value != null;
    }

    @Override
    public int getKeySize() {
        return keySize;
    }

    @Override
    public int getValueSize() {
        return valueSize;
    }
}

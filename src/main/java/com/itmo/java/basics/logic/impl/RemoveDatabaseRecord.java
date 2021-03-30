package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

public class RemoveDatabaseRecord implements WritableDatabaseRecord {
    private static final int SIZE_OF_KEY_SIZE = 4;
    private static final int SIZE_OF_VALUE_SIZE = 4;
    private static final int VALUE_SIZE = -1;
    private static final byte[] VALUE = null;
    private int keySize;
    private byte[] key;

    public RemoveDatabaseRecord(int keySize, byte[] key) {
        this.keySize = keySize;
        this.key = key;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return VALUE;
    }

    @Override
    public long size() {
        return SIZE_OF_KEY_SIZE + keySize + SIZE_OF_VALUE_SIZE;
    }

    @Override
    public boolean isValuePresented() {
        return false;
    }

    @Override
    public int getKeySize() {
        return keySize;
    }

    @Override
    public int getValueSize() {
        return VALUE_SIZE;
    }
}

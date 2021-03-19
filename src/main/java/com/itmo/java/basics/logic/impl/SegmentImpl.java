package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class SegmentImpl implements Segment {
    private final static int MAX_SIZE = 1000000;
    private SegmentIndex segmentIndex = new SegmentIndex();
    private Path pathToSegment;
    private String segmentName;
    private long curOffset = 0;

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        Path pathToSegment = Path.of(tableRootPath.toString() + "/" + segmentName);
        try {
            Files.createFile(pathToSegment);
        } catch (IOException e) {
            throw new DatabaseException("Can not create a directory", e);
        }
        return new SegmentImpl(segmentName, pathToSegment);
    }

    public SegmentImpl(String segmentName, Path pathToSegment) {
        this.pathToSegment = pathToSegment;
        this.segmentName = segmentName;
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return segmentName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        if (isReadOnly()) return false;
        DatabaseOutputStream outputStream = new DatabaseOutputStream(new FileOutputStream(pathToSegment.toString(), true));
        int writtenBytes = outputStream.write(new SetDatabaseRecord(objectKey.length(), objectKey.getBytes(StandardCharsets.UTF_8), objectValue.length, objectValue));
        segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(curOffset));
        curOffset += writtenBytes;
        outputStream.close();
        return true;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        DatabaseInputStream inputStream = new DatabaseInputStream(new FileInputStream(pathToSegment.toString()));
        var offset = segmentIndex.searchForKey(objectKey);
        if (offset.isEmpty()) return Optional.empty();
        long skippedBytes = inputStream.skip(offset.get().getOffset());
        if (skippedBytes != offset.get().getOffset())
            throw new IOException("Skipped " + skippedBytes + "bytes, when must skipped " + offset.get().getOffset());
        var databaseRecord = inputStream.readDbUnit();
        if (databaseRecord.isEmpty()) return Optional.empty();
        inputStream.close();
        return Optional.of(databaseRecord.get().getValue());
    }

    @Override
    public boolean isReadOnly() {
        return curOffset >= MAX_SIZE;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        if (segmentIndex.searchForKey(objectKey).isEmpty()) return false;
        DatabaseOutputStream outputStream = new DatabaseOutputStream(new FileOutputStream(pathToSegment.toString(), true));
        int writtenBytes = outputStream.write(new RemoveDatabaseRecord(objectKey.length(), objectKey.getBytes(StandardCharsets.UTF_8)));
        segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(curOffset));
        curOffset += writtenBytes;
        outputStream.close();
        return true;
    }
}

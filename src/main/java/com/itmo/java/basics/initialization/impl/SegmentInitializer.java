package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Optional;


public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path pathToSegment = context.currentSegmentContext().getSegmentPath();
        try {
            FileInputStream segmentInput = new FileInputStream(pathToSegment.toString());
            try (DatabaseInputStream inputStream = new DatabaseInputStream(segmentInput)) {
                SegmentIndex segmentIndex = new SegmentIndex();
                long currentSize = context.currentSegmentContext().getCurrentSize();
                ArrayList<String> keyList = new ArrayList<String>();
                Optional<DatabaseRecord> dbUnit = inputStream.readDbUnit();
                while (dbUnit.isPresent()) {
                    segmentIndex.onIndexedEntityUpdated(new String(dbUnit.get().getKey(), StandardCharsets.UTF_8),
                            new SegmentOffsetInfoImpl(currentSize));
                    keyList.add(new String(dbUnit.get().getKey(), StandardCharsets.UTF_8));
                    currentSize += dbUnit.get().size();
                    dbUnit = inputStream.readDbUnit();
                }
                SegmentInitializationContextImpl segmentContext = new SegmentInitializationContextImpl(context.currentSegmentContext().getSegmentName(),
                        context.currentSegmentContext().getSegmentPath(), (int) currentSize, segmentIndex);
                Segment segment = SegmentImpl.initializeFromContext(segmentContext);
                context.currentTableContext().updateCurrentSegment(segment);
                for (String key : keyList) {
                    context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key, segment);
                }
            } catch (IOException e) {
                throw new DatabaseException("IOException when read segment " + context.currentSegmentContext().getSegmentName());
            }
        } catch (FileNotFoundException e) {
            throw new DatabaseException("FileNotFoundException when try to read file " + pathToSegment, e);
        }
    }
}

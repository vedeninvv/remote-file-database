package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.util.Arrays;

public class TableInitializer implements Initializer {
    private SegmentInitializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *                           или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        var tableDir = new File(String.valueOf(context.currentTableContext().getTablePath()));
        if (!tableDir.exists()) {
            throw new DatabaseException("Context has incorrect path to table");
        }
        try {
            File[] files = tableDir.listFiles();
            Arrays.sort(files);
            for (var file : files) {
                var segmentContext = new SegmentInitializationContextImpl(file.getName(), context.currentTableContext().getTablePath(), 0);
                var newContext = InitializationContextImpl.builder()
                        .executionEnvironment(context.executionEnvironment())
                        .currentDatabaseContext(context.currentDbContext())
                        .currentTableContext(context.currentTableContext())
                        .currentSegmentContext(segmentContext).build();
                segmentInitializer.perform(newContext);
            }
            var table = TableImpl.initializeFromContext(context.currentTableContext());
            context.currentDbContext().addTable(table);
        } catch (SecurityException e) {
            throw new DatabaseException("Can not read content of directory " + tableDir.getAbsolutePath(), e);
        }
    }
}

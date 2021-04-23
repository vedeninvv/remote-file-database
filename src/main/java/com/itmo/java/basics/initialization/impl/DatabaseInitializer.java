package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;

public class DatabaseInitializer implements Initializer {
    private TableInitializer tableInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *                           или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        var databaseDir = new File(String.valueOf(initialContext.currentDbContext().getDatabasePath()));
        if (!databaseDir.exists()) {
            throw new DatabaseException("Context has incorrect path to database");
        }
        try {
            File[] directories = databaseDir.listFiles(File::isDirectory);
            for (var directory : directories) {
                var tableContext = new TableInitializationContextImpl(directory.getName(),
                        initialContext.currentDbContext().getDatabasePath(), new TableIndex());
                var newContext = InitializationContextImpl.builder()
                        .executionEnvironment(initialContext.executionEnvironment())
                        .currentDatabaseContext(initialContext.currentDbContext())
                        .currentTableContext(tableContext).build();
                tableInitializer.perform(newContext);
            }
            var database = DatabaseImpl.initializeFromContext(initialContext.currentDbContext());
            initialContext.executionEnvironment().addDatabase(database);
        } catch (SecurityException e) {
            throw new DatabaseException("Can not read content of directory " + databaseDir.getAbsolutePath(), e);
        }
    }
}

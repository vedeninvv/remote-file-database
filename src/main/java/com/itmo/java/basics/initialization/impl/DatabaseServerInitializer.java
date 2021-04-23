package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;

import java.io.File;

public class DatabaseServerInitializer implements Initializer {
    private DatabaseInitializer databaseInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, нацинает их инициалиализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        var workingPath = context.executionEnvironment().getWorkingPath();
        File workingDir = new File(String.valueOf(workingPath));
        if (!workingDir.exists()) {
            if (!workingDir.mkdirs()) {
                throw new DatabaseException("Exception when create directory of execution environment");
            }
        }
        File[] directories = workingDir.listFiles(File::isDirectory);
        for (var directory : directories) {
            var databaseContext = new DatabaseInitializationContextImpl(directory.getName(), workingPath);
            var newContext = InitializationContextImpl.builder()
                    .currentDatabaseContext(databaseContext)
                    .executionEnvironment(context.executionEnvironment()).build();
            databaseInitializer.perform(newContext);
        }
    }
}

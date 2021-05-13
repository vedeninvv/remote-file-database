package com.itmo.java.basics;

import com.itmo.java.basics.console.*;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.InitializationContextImpl;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespArray;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {

    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final ExecutionEnvironment env;

    /**
     * Конструктор
     *
     * @param env         env для инициализации. Далее работа происходит с заполненным объектом
     * @param initializer готовый чейн инициализации
     * @throws DatabaseException если произошла ошибка инициализации
     */
    public static DatabaseServer initialize(ExecutionEnvironment env, DatabaseServerInitializer initializer) throws DatabaseException {
        InitializationContext context = InitializationContextImpl.builder().executionEnvironment(env).build();
        initializer.perform(context);
        return new DatabaseServer(env);
    }

    private DatabaseServer(ExecutionEnvironment env){
        this.env = env;
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message) {
        return CompletableFuture.supplyAsync(() -> {
            return DatabaseCommands.valueOf(message.getObjects().get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex())
                    .asString()).getCommand(env, message.getObjects()).execute();
        }, executorService);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {
        return CompletableFuture.supplyAsync(command::execute, executorService);
    }
}
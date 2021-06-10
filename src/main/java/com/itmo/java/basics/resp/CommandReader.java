package com.itmo.java.basics.resp;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommands;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.console.impl.CreateDatabaseCommand;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;

public class CommandReader implements AutoCloseable {
    private final RespReader reader;
    private final ExecutionEnvironment env;

    public CommandReader(RespReader reader, ExecutionEnvironment env) {
        this.reader = reader;
        this.env = env;
    }

    /**
     * Есть ли следующая команда в ридере?
     */
    public boolean hasNextCommand() throws IOException {
        return reader.hasArray();
    }

    /**
     * Считывает комманду с помощью ридера и возвращает ее
     *
     * @throws IllegalArgumentException если нет имени команды и id
     */
    public DatabaseCommand readCommand() throws IOException {
        RespArray respArray = reader.readArray();
        if (respArray.getObjects().size() < DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex() + 1){
            throw new IllegalArgumentException("RespArray does not have enough size to have id, name and one object");
        }
        RespObject id = respArray.getObjects().get(DatabaseCommandArgPositions.COMMAND_ID.getPositionIndex());
        if (!(id instanceof RespCommandId)){
            throw new IllegalArgumentException("Command does not have command id");
        }
        RespObject commandName = respArray.getObjects().get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex());
        if (!(commandName instanceof RespBulkString)){
            throw new IllegalArgumentException("Command does not have command name");
        }
        if (commandName.asString() == null){
            throw new IllegalArgumentException("Command name does not exist");
        }
        return DatabaseCommands.valueOf(commandName.asString()).getCommand(env, respArray.getObjects());
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }
}

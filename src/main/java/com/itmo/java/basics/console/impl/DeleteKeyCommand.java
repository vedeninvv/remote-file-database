package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.Optional;

/**
 * Команда для создания удаления значения по ключу
 */
public class DeleteKeyCommand implements DatabaseCommand {
    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;
    private final String key;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public DeleteKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        this.env = env;
        this.databaseName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        this.tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
        this.key = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
    }

    /**
     * Удаляет значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с удаленным значением. Например, "previous"
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            Optional<Database> database = env.getDatabase(databaseName);
            if (database.isEmpty()){
                return DatabaseCommandResult.error("Not found database " + databaseName);
            }
            Optional<byte[]> value = database.get().read(tableName, key);
            if (value.isEmpty()){
                return DatabaseCommandResult.error("Value with key " + key + " in database " + databaseName + " not found");
            }
            database.get().delete(tableName, key);
            return DatabaseCommandResult.success(value.get());
        } catch (DatabaseException e){
            return DatabaseCommandResult.error("DatabaseException when try to delete value by key " + key + " in table " + tableName);
        }
    }
}

package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Команда для создания записи значения
 */
public class SetKeyCommand implements DatabaseCommand {
    private final ExecutionEnvironment env;
    private final String databaseName;
    private final String tableName;
    private final String key;
    private final String value;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ, значение
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public SetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        this.env = env;
        this.databaseName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        this.tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
        this.key = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
        this.value = commandArgs.get(DatabaseCommandArgPositions.VALUE.getPositionIndex()).asString();
    }

    /**
     * Записывает значение
     *
     * @return {@link DatabaseCommandResult#success(byte[])} c предыдущим значением. Например, "previous" или null, если такого не было
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            Optional<Database> database = env.getDatabase(databaseName);
            if (database.isEmpty()){
                return new FailedDatabaseCommandResult("Not found database " + databaseName);
            }
            Optional<byte[]> previousValue = database.get().read(tableName, key);
            database.get().write(tableName, key, value.getBytes(StandardCharsets.UTF_8));
            return new SuccessDatabaseCommandResult(previousValue.orElse("null".getBytes(StandardCharsets.UTF_8)));
        } catch (DatabaseException e){
            return new FailedDatabaseCommandResult("DatabaseException when try to set value by key " + key + " in table " +
                    tableName + " with value " + value);
        }
    }
}

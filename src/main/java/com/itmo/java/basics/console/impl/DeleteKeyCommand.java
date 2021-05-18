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
    private static final int VALID_ARGUMENTS_NUMBER = 5;
    private final ExecutionEnvironment env;
    private final List<RespObject> commandArgs;

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
        if (commandArgs.size() != VALID_ARGUMENTS_NUMBER) {
            throw new IllegalArgumentException("Wrong amount of arguments");
        }
        this.env = env;
        this.commandArgs = commandArgs;
    }

    /**
     * Удаляет значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с удаленным значением. Например, "previous"
     */
    @Override
    public DatabaseCommandResult execute() {
        Optional<Database> database =
                env.getDatabase(commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString());
        if (database.isEmpty()) {
            return DatabaseCommandResult.error(String.format("Database with name %s does not exist",
                    commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString()));
        }
        Optional<byte[]> value;
        try {
            value = database.get().read(
                    commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString(),
                    commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString());
            if (value.isEmpty()) {
                return DatabaseCommandResult.error(String.format("Value by key %s does not exist",
                        commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString()));
            }
            database.get().delete(
                    commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString(),
                    commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString());
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }
        return DatabaseCommandResult.success(value.get());
    }
}

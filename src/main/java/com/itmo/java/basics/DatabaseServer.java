package com.itmo.java.basics;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.DatabaseCommands;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.InitializationContextImpl;
import com.itmo.java.protocol.model.RespArray;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {

    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final ExecutionEnvironment env;
    private final DatabaseServerInitializer initializer;

    private DatabaseServer(ExecutionEnvironment env, DatabaseServerInitializer initializer) {
        this.env = env;
        this.initializer = initializer;
    }

    /**
     * Конструктор
     *
     * @param env         env для инициализации. Далее работа происходит с заполненным объектом
     * @param initializer готовый чейн инициализации
     * @throws DatabaseException если произошла ошибка инициализации
     */
    public static DatabaseServer initialize(ExecutionEnvironment env, DatabaseServerInitializer initializer) throws DatabaseException {
        initializer.perform(InitializationContextImpl
                .builder()
                .executionEnvironment(env)
                .build());
        return new DatabaseServer(env, initializer);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message) {
        return CompletableFuture.supplyAsync(() -> {
            DatabaseCommand command = DatabaseCommands.valueOf(message
                    .getObjects()
                    .get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex())
                    .asString())
                    .getCommand(env, message.getObjects());
            return command.execute();
        }, executorService);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {
        return CompletableFuture.supplyAsync(command::execute, executorService);
    }
}
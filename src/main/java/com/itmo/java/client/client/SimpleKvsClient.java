package com.itmo.java.client.client;

import com.itmo.java.client.command.CreateDatabaseKvsCommand;
import com.itmo.java.client.command.CreateTableKvsCommand;
import com.itmo.java.client.command.DeleteKvsCommand;
import com.itmo.java.client.command.GetKvsCommand;
import com.itmo.java.client.command.KvsCommand;
import com.itmo.java.client.command.SetKvsCommand;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespObject;

import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {
    private final String databaseName;
    private final Supplier<KvsConnection> connectionSupplier;

    /**
     * Конструктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания подключения к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
        this.databaseName = databaseName;
        this.connectionSupplier = connectionSupplier;
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        KvsCommand command = new CreateDatabaseKvsCommand(databaseName);
        RespObject result;
        try {
            result = connectionSupplier.get().send(command.getCommandId(), command.serialize());
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("ConnectionException when sending command %s",
                    command.serialize().asString()), e);
        }
        if (result.isError()) {
            throw new DatabaseExecutionException(String.format("Error when executing command %s",
                    command.serialize().asString()));
        }
        return result.asString();
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        KvsCommand command = new CreateTableKvsCommand(databaseName, tableName);
        RespObject result;
        try {
            result = connectionSupplier.get().send(command.getCommandId(), command.serialize());
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("ConnectionException when sending command %s",
                    command.serialize().asString()), e);
        }
        if (result.isError()) {
            throw new DatabaseExecutionException(String.format("Error when executing command %s",
                    command.serialize().asString()));
        }
        return result.asString();
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        KvsCommand command = new GetKvsCommand(databaseName, tableName, key);
        RespObject result;
        try {
            result = connectionSupplier.get().send(command.getCommandId(), command.serialize());
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("ConnectionException when sending command %s",
                    command.serialize().asString()), e);
        }
        if (result.isError()) {
            throw new DatabaseExecutionException(String.format("Error when executing command %s",
                    command.serialize().asString()));
        }
        return result.asString();
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        KvsCommand command = new SetKvsCommand(databaseName, tableName, key, value);
        RespObject result;
        try {
            result = connectionSupplier.get().send(command.getCommandId(), command.serialize());
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("ConnectionException when sending command %s",
                    command.serialize().asString()), e);
        }
        if (result.isError()) {
            throw new DatabaseExecutionException(String.format("Error when executing command %s",
                    command.serialize().asString()));
        }
        return result.asString();
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        KvsCommand command = new DeleteKvsCommand(databaseName, tableName, key);
        RespObject result;
        try {
            result = connectionSupplier.get().send(command.getCommandId(), command.serialize());
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("ConnectionException when sending command %s",
                    command.serialize().asString()), e);
        }
        if (result.isError()) {
            throw new DatabaseExecutionException(String.format("Error when executing command %s",
                    command.serialize().asString()));
        }
        return result.asString();
    }
}

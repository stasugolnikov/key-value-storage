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
        try {
            return connectionSupplier.get().send(command.getCommandId(), command.serialize()).asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("ConnectionException when sending command %s",
                    command.serialize().asString()), e);
        }
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        KvsCommand command = new CreateTableKvsCommand(databaseName, tableName);
        try {
            return connectionSupplier.get().send(command.getCommandId(), command.serialize()).asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("ConnectionException when sending command %s",
                    command.serialize().asString()), e);
        }
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        KvsCommand command = new GetKvsCommand(databaseName, tableName, key);
        try {
            return connectionSupplier.get().send(command.getCommandId(), command.serialize()).asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("ConnectionException when sending command %s",
                    command.serialize().asString()), e);
        }
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        KvsCommand command = new SetKvsCommand(databaseName, tableName, key, value);
        try {
            return connectionSupplier.get().send(command.getCommandId(), command.serialize()).asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("ConnectionException when sending command %s",
                    command.serialize().asString()), e);
        }
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        KvsCommand command = new DeleteKvsCommand(databaseName, tableName, key);
        try {
            return connectionSupplier.get().send(command.getCommandId(), command.serialize()).asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("ConnectionException when sending command %s",
                    command.serialize().asString()), e);
        }
    }
}

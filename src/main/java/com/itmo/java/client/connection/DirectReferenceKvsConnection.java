package com.itmo.java.client.connection;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Реализация подключения, когда есть прямая ссылка на объект
 * (пока еще нет реализации сокетов)
 */
public class DirectReferenceKvsConnection implements KvsConnection {
    private final DatabaseServer databaseServer;

    public DirectReferenceKvsConnection(DatabaseServer databaseServer) {
        this.databaseServer = databaseServer;
    }

    @Override
    public RespObject send(int commandId, RespArray command) throws ConnectionException {
        CompletableFuture<DatabaseCommandResult> dbCommandResult =
                databaseServer.executeNextCommand(new RespArray(new RespCommandId(commandId), command));
        try {
            return dbCommandResult.get().serialize();
        } catch (ExecutionException e) {
            throw new ConnectionException("ExecutionException when getting database command result", e);
        } catch (InterruptedException e) {
            throw new ConnectionException("InterruptedException when getting database command result", e);
        }
    }

    /**
     * Ничего не делает ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
    }
}

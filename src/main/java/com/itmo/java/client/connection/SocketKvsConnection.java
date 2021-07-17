package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.net.Socket;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {
    private final ConnectionConfig config;
    private final Socket socket;
    private final RespWriter respWriter;
    private final RespReader respReader;

    public SocketKvsConnection(ConnectionConfig config) {
        this.config = config;
        try {
            this.socket = new Socket(config.getHost(), config.getPort());
        } catch (IOException e) {
            throw new RuntimeException(String.format("IOException when opening socket on host %s and port %s",
                    config.getHost(), config.getPort()), e);
        }
        try {
            this.respWriter = new RespWriter(socket.getOutputStream());
        } catch (IOException e) {
            throw new RuntimeException(String.format("IOException when getting OutputStream in socket %s", socket), e);
        }
        try {
            this.respReader = new RespReader(socket.getInputStream());
        } catch (IOException e) {
            throw new RuntimeException(String.format("IOException when getting InputStream in socket %s", socket), e);
        }
    }

    /**
     * Отправляет с помощью сокета команду и получает результат.
     *
     * @param commandId id команды (номер)
     * @param command   команда
     * @throws ConnectionException если сокет закрыт или если произошла другая ошибка соединения
     */
    @Override
    public synchronized RespObject send(int commandId, RespArray command) throws ConnectionException {
        if (socket.isClosed()) {
            throw new ConnectionException(String.format("Socket %s is closed", socket));
        }
        try {
            respWriter.write(command);
        } catch (IOException e) {
            throw new ConnectionException(String.format("IOException when writing command in socket %s", socket), e);
        }
        try {
            return respReader.readObject();
        } catch (IOException e) {
            throw new ConnectionException(String.format("IOException when reading result from socket %s", socket), e);
        }
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException e1) {
            try {
                respReader.close();
            } catch (IOException e2) {
                try {
                    respWriter.close();
                } catch (IOException e3) {
                    throw new RuntimeException(String.format("IOException when closing resp writer %s", respWriter), e3);
                }
                throw new RuntimeException(String.format("IOException when closing resp reader %s", respReader), e2);
            }
            throw new RuntimeException(String.format("IOException when closing socket %s", socket), e1);
        }
        try {
            respReader.close();
        } catch (IOException e1) {
            try {
                respWriter.close();
            } catch (IOException e2) {
                throw new RuntimeException(String.format("IOException when closing resp writer %s", respWriter), e2);
            }
            throw new RuntimeException(String.format("IOException when closing resp reader %s", respReader), e1);
        }
        try {
            respWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(String.format("IOException when closing resp writer %s", respWriter), e);
        }
    }
}

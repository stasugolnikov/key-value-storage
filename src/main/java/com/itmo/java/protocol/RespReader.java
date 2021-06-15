package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class RespReader implements AutoCloseable {

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    private final InputStream is;

    public RespReader(InputStream is) {
        this.is = is;
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        byte code = (byte) is.read();
        return code == RespArray.CODE;
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        byte code = (byte) is.read();
        switch (code) {
            case RespArray.CODE:
                return readArray();
            case RespBulkString.CODE:
                return readBulkString();
            case RespCommandId.CODE:
                return readCommandId();
            case RespError.CODE:
                return readError();
            default:
                throw new IOException(); // todo message
        }
    }

    private int readInt() throws IOException {
        StringBuilder result = new StringBuilder();
        byte b;
        while ((b = (byte) is.read()) != CR) {
            result.append(b);
        }
        byte lf = (byte) is.read();
        if (lf != LF) {
            throw new IOException(); // todo message
        }
        return Integer.parseInt(String.valueOf(result));
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        StringBuilder result = new StringBuilder();
        char ch;
        while ((ch = (char) is.read()) != CR) {
            result.append(ch);
        }
        byte lf = (byte) is.read();
        if (lf != LF) {
            throw new IOException(); // todo message
        }
        return new RespError(String.valueOf(result).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        int size = readInt();
        if (size == -1) {
            return RespBulkString.NULL_STRING;
        }
        byte[] data = is.readNBytes(size);
        byte[] crlf = is.readNBytes(2);
//        if (crlf[0] != CR && crlf[1] != LF) {
//            throw new IOException(); // todo message
//        }
        return new RespBulkString(data);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        int size = readInt();
        RespArray respArray = new RespArray(readObject());
        for (int i = 1; i < size; i++) {
            respArray.getObjects().add(readObject());
        }
        return respArray;
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        return new RespCommandId(readInt());
    }


    @Override
    public void close() throws IOException {
        is.close();
    }
}

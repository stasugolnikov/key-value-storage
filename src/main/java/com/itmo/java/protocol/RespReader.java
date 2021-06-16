package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        if (code == -1) {
            throw new EOFException();
        }
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
        if (code == -1) {
            throw new EOFException(); // todo message
        }
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
        List<Byte> result = new ArrayList<>();
        byte b;
        while ((b = (byte) is.read()) != CR) {
            if (b == -1) {
                throw new EOFException();
            }
            result.add(b);
        }
        byte ignoreLf = (byte) is.read();
        byte[] byteArray = new byte[result.size()];
        for (int i = 0; i < result.size(); i++) {
            byteArray[i] = result.get(i);
        }
        return Integer.parseInt(new String(byteArray));
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
            if (ch == 65535) {
                throw new EOFException();
            }
            result.append(ch);
        }
        byte ignoreLf = (byte) is.read();
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
        if (data.length != size) {
            throw new EOFException();
        }
        byte[] ignoreCrlf = is.readNBytes(RespObject.CRLF.length);
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
        RespObject[] objects = new RespObject[size];
        for (int i = 0; i < size; i++) {
            objects[i] = readObject();
        }
        return new RespArray(objects);
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        int id = ByteBuffer.wrap(is.readNBytes(Integer.BYTES)).getInt();
        byte[] ignoreCrlf = is.readNBytes(RespObject.CRLF.length);
        return new RespCommandId(id);
    }


    @Override
    public void close() throws IOException {
        is.close();
    }
}

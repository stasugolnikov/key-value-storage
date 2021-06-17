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
import java.util.List;

public class RespReader implements AutoCloseable {
    private static final int END_OF_STREAM = -1;

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
        if (code == END_OF_STREAM) {
            throw new EOFException(String.format("End of file in stream %s", is));
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
        if (code == END_OF_STREAM) {
            throw new EOFException(String.format("End of file in stream %s", is));
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
                throw new IOException("Unknown RESP object code");
        }
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
            if ((byte) ch == END_OF_STREAM) {
                throw new EOFException(String.format("End of file in stream %s", is));
            }
            result.append(ch);
        }
        readLf();
        return new RespError(String.valueOf(result).getBytes(StandardCharsets.UTF_8));
    }

    private int readSize() throws IOException {
        List<Byte> result = new ArrayList<>();
        byte b;
        while ((b = (byte) is.read()) != CR) {
            if (b == END_OF_STREAM) {
                throw new EOFException(String.format("End of file in stream %s", is));
            }
            result.add(b);
        }
        readLf();
        byte[] byteArray = new byte[result.size()];
        for (int i = 0; i < result.size(); i++) {
            byteArray[i] = result.get(i);
        }
        return Integer.parseInt(new String(byteArray));
    }

    private void readCr() throws IOException {
        byte cr = (byte) is.read();
        if (cr == END_OF_STREAM) {
            throw new EOFException(String.format("End of file in stream %s", is));
        }
        if (cr != CR) {
            throw new IOException(String.format("Error when CR expected but was read %s", cr));
        }
    }

    private void readLf() throws IOException {
        byte lf = (byte) is.read();
        if (lf == END_OF_STREAM) {
            throw new EOFException(String.format("End of file in stream %s", is));
        }
        if (lf != LF) {
            throw new IOException(String.format("Error when LF expected but was read %s", lf));
        }
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        int size = readSize();
        if (size == END_OF_STREAM) {
            return RespBulkString.NULL_STRING;
        }
        byte[] data = is.readNBytes(size);
        if (data.length != size) {
            throw new EOFException(String.format("End of file in stream %s", is));
        }
        readCr();
        readLf();
        return new RespBulkString(data);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        int size = readSize();
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
        if (id == END_OF_STREAM) {
            throw new EOFException(String.format("End of file in stream %s", is));
        }
        readCr();
        readLf();
        return new RespCommandId(id);
    }


    @Override
    public void close() throws IOException {
        is.close();
    }
}

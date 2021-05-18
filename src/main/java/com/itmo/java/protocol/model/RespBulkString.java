package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Строка
 */
public class RespBulkString implements RespObject {
    /**
     * Код объекта
     */
    public static final byte CODE = '$';

    public static final int NULL_STRING_SIZE = -1;
    private final byte[] data;

    public RespBulkString(byte[] data) {
        this.data = data;
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() {
        return false;
    }

    /**
     * Строковое представление
     *
     * @return строку, если данные есть. Если нет - null
     */
    @Override
    public String asString() {
        return data != null ? new String(data) : null;
    }

    private byte[] intToByteArray(int value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(value);
        return byteBuffer.array();
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        if (data == null) {
            os.write(intToByteArray(NULL_STRING_SIZE));
            os.write(CRLF);
            return;
        }
        os.write(intToByteArray(data.length));
        os.write(CRLF);
        os.write(data);
        os.write(CRLF);
    }
}

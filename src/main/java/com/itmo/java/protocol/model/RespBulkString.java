package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
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

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        if (data == null) {
            os.write(Integer.toString(NULL_STRING_SIZE).getBytes(StandardCharsets.UTF_8));
            os.write(CRLF);
            return;
        }
        os.write(Integer.toString(data.length).getBytes(StandardCharsets.UTF_8));
        os.write(CRLF);
        os.write(data);
        os.write(CRLF);
    }
}

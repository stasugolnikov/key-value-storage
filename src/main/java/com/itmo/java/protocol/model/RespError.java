package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Сообщение об ошибке в RESP протоколе
 */
public class RespError implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '-';
    private final byte[] message;

    public RespError(byte[] message) {
        this.message = message;
    }

    /**
     * Ошибка ли это? Ответ - да
     *
     * @return true
     */
    @Override
    public boolean isError() {
        return true;
    }

    @Override
    public String asString() {
        return message != null ? new String(message) : null;
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(message);
        os.write(CRLF);
    }
}

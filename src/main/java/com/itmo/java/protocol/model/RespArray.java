package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Массив RESP объектов
 */
public class RespArray implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '*';
    private final List<RespObject> objects;

    public RespArray(RespObject... objects) {
        this.objects = objects != null ? List.of(objects) : null;
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
     * @return результаты метода {@link RespObject#asString()} для всех хранимых объектов, разделенные пробелом
     */
    @Override
    public String asString() {
        if (objects == null) {
            return null;
        }
        return objects.stream().map(RespObject::asString).collect(Collectors.joining(" "));
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(objects.size());
        os.write(byteBuffer.array());
        os.write(CRLF);
        for (RespObject object : objects) {
            object.write(os);
        }
    }

    public List<RespObject> getObjects() {
        return objects;
    }
}

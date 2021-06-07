package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Массив RESP объектов
 */
public class RespArray implements RespObject {
    private final List<RespObject> objects;

    /**
     * Код объекта
     */
    public static final byte CODE = '*';

    public RespArray(RespObject... objects) {
        this.objects = Arrays.asList(objects);
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
        StringBuilder stringBuilder = new StringBuilder();
        for (RespObject object : objects) {
            stringBuilder.append(object.asString());
            stringBuilder.append(" ");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(String.valueOf(objects.size()).getBytes(StandardCharsets.UTF_8));
        os.write(CRLF);
        os.flush();
        for (RespObject object : objects) {
            object.write(os);
        }
    }

    public List<RespObject> getObjects() {
        return objects;
    }
}

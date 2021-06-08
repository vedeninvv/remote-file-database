package com.itmo.java.protocol;

import com.itmo.java.protocol.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class RespReader implements AutoCloseable {
    private static final int READ_AHEAD_LIMIT = 3;
    private final InputStreamReader reader;

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    public RespReader(InputStream is) {
        reader = new InputStreamReader(is);
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        BufferedReader bufferedReader = new BufferedReader(reader);
        bufferedReader.mark(READ_AHEAD_LIMIT);
        byte code = (byte) reader.read();
        bufferedReader.reset();
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

        int codeInt = reader.read();
        if (codeInt == -1) {
            throw new EOFException("InputStream is empty when try to read RespObject");
        }
        byte code = (byte) codeInt;
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
                throw new IOException("Code character is not correct");
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        return new RespError(readBytesToEndOfLine());
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        byte[] stringSizeBytes = readBytesToEndOfLine();
        int stringSize = Integer.parseInt(new String(stringSizeBytes, StandardCharsets.UTF_8));
        if (stringSize == RespBulkString.NULL_STRING_SIZE) {
            return RespBulkString.NULL_STRING;
        }
        byte[] stringData = readBytesToEndOfLine();
        if (stringData.length != stringSize) {
            throw new IOException("String length is not equal with StringBulk size");
        }
        return new RespBulkString(stringData);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        byte[] arraySizeBytes = readBytesToEndOfLine();
        int arraySize = Integer.parseInt(new String(arraySizeBytes, StandardCharsets.UTF_8));
        RespObject[] respObjectArray = new RespObject[arraySize];
        for (int i = 0; i < arraySize; i++) {
            respObjectArray[i] = readObject();
        }
        return new RespArray(respObjectArray);
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        byte[] idBytes = readBytesToEndOfLine();
        if (idBytes.length != 4) {
            throw new IOException("Command Id is not integer");
        }
        return new RespCommandId(bytesToInt(idBytes));
    }


    @Override
    public void close() throws IOException {
        reader.close();
    }

    private static int bytesToInt(byte[] bytes) {
        return (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | (bytes[3]);
    }

    private byte[] readBytesToEndOfLine() throws IOException {
        ArrayList<Byte> message = new ArrayList<>();
        while (true) {
            int currentByte = reader.read();
            if (currentByte == -1) {
                throw new EOFException("Stream is empty when try to read all bytes before '\\r\\n'");
            }
            if (currentByte == CR) {
                int nextByte = reader.read();
                if (nextByte == -1) {
                    throw new EOFException("Stream is empty when try to read all bytes before '\\r\\n'");
                }
                if (nextByte == LF){
                    break;
                }else {
                    message.add((byte) currentByte);
                    currentByte = nextByte;
                }
            }
            message.add((byte) currentByte);
        }
        byte[] bytes = new byte[message.size()];
        for (int i = 0; i < message.size(); i++) {
            bytes[i] = message.get(i);
        }
        return bytes;
    }
}

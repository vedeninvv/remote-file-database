package com.itmo.java.protocol;

import com.itmo.java.protocol.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class RespReader implements AutoCloseable {
    private static final int READ_AHEAD_LIMIT = 3;
    private final BufferedReader bufferedReader;

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    public RespReader(InputStream is) {
        bufferedReader = new BufferedReader(new InputStreamReader(is));
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        bufferedReader.mark(READ_AHEAD_LIMIT);
        byte code = (byte) bufferedReader.read();
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
        int codeInt = bufferedReader.read();
        if (codeInt == -1) {
            throw new EOFException("InputStream is empty when try to read RespObject");
        }
        byte code = (byte) codeInt;
        switch (code) {
            case RespArray.CODE:
                var result1 = readArray();
                if (result1 == null){
                    throw new NullPointerException("RespArray!!");
                }
                return result1;
            case RespBulkString.CODE:
                var result2 = readBulkString();
                if (result2 == null){
                    throw new NullPointerException("RespBulkString!!");
                }
                return result2;
            case RespCommandId.CODE:
                var result3 = readCommandId();
                if (result3 == null){
                    throw new NullPointerException("CommandID!!");
                }
                return result3;
            case RespError.CODE:
                var result4 = readError();
                if (result4 == null){
                    throw new NullPointerException("RespError!!");
                }
                return result4;
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
        String message = bufferedReader.readLine();
        if (message.isEmpty()) {
            throw new EOFException("InputStream is empty when try to read Error");
        }
        return new RespError(message.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        String stringSizeStr = bufferedReader.readLine();
        if (stringSizeStr.isEmpty()) {
            throw new EOFException("InputStream is empty when try to read BulkString");
        }
        int stringSize = Integer.parseInt(stringSizeStr);
        if (stringSize == RespBulkString.NULL_STRING_SIZE) {
            return RespBulkString.NULL_STRING;
        }
        String stringData = bufferedReader.readLine();
        if (stringData.length() != stringSize) {
            throw new IOException("String length is not equal with StringBulk size");
        }
        return new RespBulkString(stringData.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        String arraySizeStr = bufferedReader.readLine();
        if (arraySizeStr.isEmpty()) {
            throw new EOFException("InputStream is empty when try to read Array");
        }
        int arraySize = Integer.parseInt(arraySizeStr);
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
        String idStr = bufferedReader.readLine();
        if (idStr.isEmpty()) {
            throw new EOFException("InputStream is empty when try to read CommandId");
        }
        return new RespCommandId(bytesToInt(idStr.getBytes(StandardCharsets.UTF_8)));
    }


    @Override
    public void close() throws IOException {
        bufferedReader.close();
    }

    private static int bytesToInt(byte[] bytes) {
        return (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | (bytes[3]);
    }
}

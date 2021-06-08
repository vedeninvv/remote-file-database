package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.net.Socket;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {
    private final int port;
    private final String host;
    private final Socket clientSocket;

    public SocketKvsConnection(ConnectionConfig config) {
        this.port = config.getPort();
        this.host = config.getHost();
        try {
            this.clientSocket = new Socket(host, port);
        } catch (IOException e) {
            throw new RuntimeException("IOException when try to connect by " + host + " " + port, e);
        }
    }

    /**
     * Отправляет с помощью сокета команду и получает результат.
     *
     * @param commandId id команды (номер)
     * @param command   команда
     * @throws ConnectionException если сокет закрыт или если произошла другая ошибка соединения
     */
    @Override
    public synchronized RespObject send(int commandId, RespArray command) throws ConnectionException {
        try (RespWriter respWriter = new RespWriter(clientSocket.getOutputStream());
             RespReader respReader = new RespReader(clientSocket.getInputStream())) {
            respWriter.write(command);
            return respReader.readObject();
        } catch (IOException e) {
            throw new ConnectionException("IOException when send " + command.asString() + " with " + host + " and port " + port + " ___IOMessage___: " + e.getMessage(), e);
        }
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            clientSocket.close();
        } catch (IOException e) {
            throw new RuntimeException("IOException when try to close client socket");
        }
    }
}

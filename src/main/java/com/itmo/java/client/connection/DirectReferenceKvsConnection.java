package com.itmo.java.client.connection;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

import java.util.concurrent.ExecutionException;

/**
 * Реализация подключения, когда есть прямая ссылка на объект
 * (пока еще нет реализации сокетов)
 */
public class DirectReferenceKvsConnection implements KvsConnection {
    private final DatabaseServer databaseServer;

    public DirectReferenceKvsConnection(DatabaseServer databaseServer) {
        this.databaseServer = databaseServer;
    }

    @Override
    public RespObject send(int commandId, RespArray command) throws ConnectionException {
        RespObject[] respObjects = new RespObject[command.getObjects().size() + 1];
        respObjects[0] = new RespCommandId(commandId);
        for (int i = 0; i < command.getObjects().size(); i++) {
            respObjects[i + 1] = command.getObjects().get(i);
        }
        RespArray message = new RespArray(respObjects);
        try {
            return databaseServer.executeNextCommand(message).get().serialize();
        } catch (InterruptedException e) {
            throw new ConnectionException("ConnectionException when try to get result from server because of interruption when message is '" +
                    message.asString() + "'", e);
        } catch (ExecutionException e) {
            throw new ConnectionException("ConnectionException when try to get result from server because of ExecutionException when message is '" +
                    message.asString() + "'", e);
        }
    }

    /**
     * Ничего не делает ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
    }
}

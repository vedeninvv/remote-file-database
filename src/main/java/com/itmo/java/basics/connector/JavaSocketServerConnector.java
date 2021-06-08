package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ConfigLoader;
import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.config.DatabaseServerConfig;
import com.itmo.java.basics.config.ServerConfig;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.initialization.impl.DatabaseInitializer;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.SegmentInitializer;
import com.itmo.java.basics.initialization.impl.TableInitializer;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Класс, который предоставляет доступ к серверу через сокеты
 */
public class JavaSocketServerConnector implements Closeable {

    /**
     * Экзекьютор для выполнения ClientTask
     */
    private final ExecutorService clientIOWorkers = Executors.newSingleThreadExecutor();

    private final ServerSocket serverSocket;
    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();

    private final DatabaseServer server;

    /**
     * Стартует сервер. По аналогии с сокетом открывает коннекшн в конструкторе.
     */
    public JavaSocketServerConnector(DatabaseServer databaseServer, ServerConfig config) throws IOException {
        this.serverSocket = new ServerSocket(config.getPort());
        this.server = databaseServer;
    }

    /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public void start() {
        connectionAcceptorExecutor.submit(() -> {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                clientIOWorkers.submit(new ClientTask(clientSocket, server));
            }
        });
    }

    /**
     * Закрывает все, что нужно ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
        System.out.println("Stopping socket connector");
        connectionAcceptorExecutor.shutdownNow();
        clientIOWorkers.shutdownNow();
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                throw new RuntimeException("IOException when try to close connection", e);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        DatabaseServerConfig config = new ConfigLoader().readConfig();
        System.out.println(config.getDbConfig().getWorkingPath());
        System.out.println(config.getServerConfig().getPort());
        System.out.println(config.getServerConfig().getHost());
        DatabaseServer server = DatabaseServer.initialize(new ExecutionEnvironmentImpl(config.getDbConfig()),
                new DatabaseServerInitializer(new DatabaseInitializer(new TableInitializer(new SegmentInitializer()))));
        JavaSocketServerConnector connector = new JavaSocketServerConnector(server, config.getServerConfig());
        connector.start();
    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {
        private final Socket client;
        private final DatabaseServer server;
        private final CommandReader commandReader;
        private final RespWriter respWriter;

        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            this.client = client;
            this.server = server;
            try {
                this.commandReader = new CommandReader(new RespReader(client.getInputStream()), server.getEnv());
                this.respWriter = new RespWriter(client.getOutputStream());
            } catch (IOException e){
                throw new RuntimeException("IOException when open socket streams", e);
            }
        }

        /**
         * Исполняет задачи из одного клиентского сокета, пока клиент не отсоединился или текущий поток не был прерван (interrupted).
         * Для кажной из задач:
         * 1. Читает из сокета команду с помощью {@link CommandReader}
         * 2. Исполняет ее на сервере
         * 3. Записывает результат в сокет с помощью {@link RespWriter}
         */
        @Override
        public void run() {
            try {
                while (commandReader.hasNextCommand()) {
                    CompletableFuture<DatabaseCommandResult> commandResult = server.executeNextCommand(commandReader.readCommand());
                    respWriter.write(commandResult.get().serialize());
                }
            } catch (Exception e) {
                close();
                throw new RuntimeException("When try to read, write or execute command", e);
            }
        }

        /**
         * Закрывает клиентский сокет
         */
        @Override
        public void close() {
            try {
                client.close();
            } catch (IOException e){
                throw new RuntimeException("When try to close client connection", e);
            }
        }
    }
}

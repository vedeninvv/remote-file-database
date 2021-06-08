package com.itmo.java.basics.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {
    private static final String DEFAULT_PROPERTY_FILE = "server.properties";

    private InputStream propertyInputStream;

    /**
     * По умолчанию читает из server.properties
     */
    public ConfigLoader() {
        propertyInputStream = getClass().getClassLoader().getResourceAsStream(DEFAULT_PROPERTY_FILE);
        if (this.propertyInputStream == null) {
            try {
                this.propertyInputStream = new FileInputStream(DEFAULT_PROPERTY_FILE);
            } catch (FileNotFoundException e) {
                this.propertyInputStream = null;
            }
        }
    }

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) {
        this.propertyInputStream = getClass().getClassLoader().getResourceAsStream(DEFAULT_PROPERTY_FILE);
        if (this.propertyInputStream == null) {
            try {
                this.propertyInputStream = new FileInputStream(name);
            } catch (FileNotFoundException e) {
                this.propertyInputStream = null;
            }
        }
    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного файла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти)
     */
    public DatabaseServerConfig readConfig() {
        Properties properties = new Properties();
        try {
            if (propertyInputStream == null) {
                throw new IOException("Config file not found");
            }
            properties.load(propertyInputStream);
            String workingPath = properties.getProperty("kvs.workingPath");
            String host = properties.getProperty("kvs.host");
            String portStr = properties.getProperty("kvs.port");
            DatabaseConfig databaseConfig;
            ServerConfig serverConfig;
            if (workingPath == null) {
                databaseConfig = new DatabaseConfig();
            } else {
                databaseConfig = new DatabaseConfig(workingPath);
            }
            if (host == null || portStr == null){
                serverConfig = new ServerConfig(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT);
            }
            else {
                try {
                    int port = Integer.parseInt(portStr);
                    serverConfig = new ServerConfig(host, port);
                } catch (NumberFormatException e) {
                    serverConfig = new ServerConfig(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT);
                }
            }
            return DatabaseServerConfig.builder()
                    .dbConfig(databaseConfig)
                    .serverConfig(serverConfig)
                    .build();
        } catch (IOException e) {
            return DatabaseServerConfig.builder()
                    .dbConfig(new DatabaseConfig())
                    .serverConfig(new ServerConfig(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT))
                    .build();
        }
    }
}

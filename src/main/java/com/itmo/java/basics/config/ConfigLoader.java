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
    }

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) {
        this.propertyInputStream = getClass().getClassLoader().getResourceAsStream(DEFAULT_PROPERTY_FILE);
        if (this.propertyInputStream == null){
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
            if (propertyInputStream == null){
                throw new IOException("Config file not found");
            }
            properties.load(propertyInputStream);
            String workingPath = properties.getProperty("kvs.workingPath");
            String host = properties.getProperty("kvs.host");
            int port;
            if (workingPath == null || host == null){
                throw new IOException("WorkingPath or host is null");
            }
            try {
                port = Integer.parseInt(properties.getProperty("kvs.port"));
            } catch (NumberFormatException e){
                throw new IOException("Port is not an integer");
            }
            return DatabaseServerConfig.builder()
                    .dbConfig(new DatabaseConfig(workingPath))
                    .serverConfig(new ServerConfig(host, port))
                    .build();
        } catch (IOException e) {
            return DatabaseServerConfig.builder()
                    .dbConfig(new DatabaseConfig())
                    .serverConfig(new ServerConfig(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT))
                    .build();
        }
    }
}

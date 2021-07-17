package com.itmo.java.basics.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {
    private final String fileName;

    /**
     * По умолчанию читает из server.properties
     */
    public ConfigLoader() {
        this.fileName = "server.properties";
    }

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) {
        this.fileName = name;
    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного файла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти)
     */
    public DatabaseServerConfig readConfig() {
        URL resource = this.getClass().getClassLoader().getResource(fileName);
        String filePath = resource != null ? resource.getPath() : fileName;
        try (InputStream is = new FileInputStream(filePath)) {
            Properties properties = new Properties();
            properties.load(is);
            String workingPath = properties.getProperty("kvs.workingPath", DatabaseConfig.DEFAULT_WORKING_PATH);
            String host = properties.getProperty("kvs.host", ServerConfig.DEFAULT_HOST);
            String port = properties.getProperty("kvs.port", String.valueOf(ServerConfig.DEFAULT_PORT));
            return DatabaseServerConfig
                    .builder()
                    .serverConfig(new ServerConfig(host, Integer.parseInt(port)))
                    .dbConfig(new DatabaseConfig(workingPath))
                    .build();
        } catch (IOException ignored) {
            return DatabaseServerConfig
                    .builder()
                    .serverConfig(new ServerConfig(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT))
                    .dbConfig(new DatabaseConfig(DatabaseConfig.DEFAULT_WORKING_PATH))
                    .build();
        }
    }
}

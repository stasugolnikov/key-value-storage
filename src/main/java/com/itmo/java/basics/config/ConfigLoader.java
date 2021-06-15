package com.itmo.java.basics.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {
    private final Path filePath;

    /**
     * По умолчанию читает из server.properties
     */
    public ConfigLoader() {
        this.filePath = Path.of(
                "src" + File.separator +
                        "main" + File.separator +
                        "resources" + File.separator +
                        "server.properties");
    }

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) {
        this.filePath = Path.of(name);
    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного файла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти)
     */
    public DatabaseServerConfig readConfig() {
        try (InputStream is = new FileInputStream(filePath.toString())) {
            Properties properties = new Properties();
            properties.load(is);
            String workingPath = properties.getProperty("kvs.workingPath") != null ?
                    properties.getProperty("kvs.workingPath") : DatabaseConfig.DEFAULT_WORKING_PATH;
            String host = properties.getProperty("kvs.host") != null ?
                    properties.getProperty("kvs.host") : ServerConfig.DEFAULT_HOST;
            String port = properties.getProperty("kvs.port") != null ?
                    properties.getProperty("kvs.port") : String.valueOf(ServerConfig.DEFAULT_PORT);
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

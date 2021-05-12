package com.itmo.java.basics.config;

import java.nio.file.Paths;

public class DatabaseConfig {
    public static final String DEFAULT_WORKING_PATH = "db_files";
    private final String workingPath;

    public DatabaseConfig(String workingPath) {
        this.workingPath = workingPath;
    }

    public DatabaseConfig(){
        this.workingPath = Paths.get(System.getProperty("user.dir"), DEFAULT_WORKING_PATH).toString();
    }

    public String getWorkingPath() {
        return workingPath;
    }
}

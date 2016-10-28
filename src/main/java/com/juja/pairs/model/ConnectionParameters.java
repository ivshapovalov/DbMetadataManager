package com.juja.pairs.model;

import com.juja.pairs.view.FileView;
import com.juja.pairs.view.View;

public class ConnectionParameters {
    private String dbType;
    private String ipHost;
    private String ipPort;
    private String dbName;
    private String dbUser;
    private String dbPassword;
    private String dbTableName;

    public static ConnectionParameters parseFromFile(String fileName) {
        View view = new FileView(fileName);
        String fileContent = view.read();
        String [] paramArray = fileContent.split("\\n");
        //TODO check amount of parameters
        String dbType = paramArray[0];
        String ipHost = paramArray[1];
        String ipPort = paramArray[2];
        String dbName = paramArray[3];
        String dbUser = paramArray[4];
        String dbPassword = paramArray[5];
        String dbTableName = paramArray[6];

        return new ConnectionParameters.Builder()
                .addDbType(dbType)
                .addIpHost(ipHost)
                .addIpPort(ipPort)
                .addDbName(dbName)
                .addDbUser(dbUser)
                .addDbPassword(dbPassword)
                .addDbTableName(dbTableName)
                .build();
    }

    public String getDbName() {
        return dbName;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public String getDbTableName() {
        return dbTableName;
    }

    public String getDbType() {
        return dbType;
    }

    public String getDbUser() {
        return dbUser;
    }

    public String getIpHost() {
        return ipHost;
    }

    public String getIpPort() {
        return ipPort;
    }

    private ConnectionParameters(Builder builder) {
        this.dbType = builder.dbType;
        this.ipHost = builder.ipHost;
        this.ipPort = builder.ipPort;
        this.dbName = builder.dbName;
        this.dbUser = builder.dbUser;
        this.dbPassword = builder.dbPassword;
        this.dbTableName = builder.dbTableName;
    }

    public static class Builder {
        private String dbType;
        private String ipHost;
        private String ipPort;
        private String dbName;
        private String dbUser;
        private String dbPassword;
        private String dbTableName;

        public Builder() {

        }

        public Builder addDbType(String dbType) {
            this.dbType = dbType;
            return this;
        }

        public Builder addIpHost(String ipHost) {
            this.ipHost = ipHost;
            return this;
        }

        public Builder addIpPort(String ipPort) {
            this.ipPort = ipPort;
            return this;
        }

        public Builder addDbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public Builder addDbUser(String dbUser) {
            this.dbUser = dbUser;
            return this;
        }

        public Builder addDbPassword(String dbPassword) {
            this.dbPassword = dbPassword;
            return this;
        }

        public Builder addDbTableName(String dbTableName) {
            this.dbTableName = dbTableName;
            return this;
        }

        public ConnectionParameters build() {
            return new ConnectionParameters(this);
        }
    }
}
package com.juja.pairs.controller;

import com.juja.pairs.model.ConnectionParameters;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class PostgreSQLMetadataReader extends SQLMetadataReader {
    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex){
            ex.printStackTrace();
        }
    }

    //TODO Все методы
    public PostgreSQLMetadataReader(ConnectionParameters parameters) {
        super(parameters);
        String dbType = parameters.getDbType().toLowerCase();
        String host = parameters.getIpHost();
        String port = parameters.getIpPort();
        String dbName = parameters.getDbName();
        String url = String.format("jdbc:%s://%s:%s/%s",dbType,host,port,dbName);
        String user = parameters.getDbUser();
        String password = parameters.getDbPassword();

        try {
            connection = DriverManager.getConnection(url,user,password);
        } catch (SQLException ex){
            ex.printStackTrace();
        }
    }

    @Override
    public String read() {
        return null;
    }

    @Override
    public void close() throws IOException {
        try {
            if (connection!=null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<String> getTableColumnsWithDescription() {
        return null;
    }

    @Override
    public String getTableComment() {
        return null;
    }

    @Override
    public List<String> getTableIndexesWithDescription() {
        return null;
    }

    @Override
    public List<String> getTableForeignKeysWithDescription() {
        return null;
    }

    @Override
    public String queryCreateTables() {
        return null;
    }
}

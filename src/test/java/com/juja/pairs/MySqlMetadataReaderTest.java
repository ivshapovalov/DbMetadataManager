package com.juja.pairs;


import com.juja.pairs.controller.MySQLMetadataReader;
import com.juja.pairs.model.ConnectionParameters;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;


public class MySqlMetadataReaderTest {
    private final static String DB_TYPE = "MySQL";
    private final static String IP_HOST = "127.0.0.1";
    private final static String IP_PORT = "3306";
    private final static String DB_NAME = "test";
    private final static String DB_USER = "root";
    private final static String DB_PASSWORD = "root";
    private final static String DB_TABLE_NAME = "study";
    static MySQLMetadataReader metadataReader;

    @BeforeClass
    public static void init() {
        ConnectionParameters parameters = new ConnectionParameters.Builder()
                .addDbType(DB_TYPE)
                .addIpHost(IP_HOST)
                .addIpPort(IP_PORT)
                .addDbName(DB_NAME)
                .addDbUser(DB_USER)
                .addDbPassword(DB_PASSWORD)
                .addDbTableName(DB_TABLE_NAME)
                .build();
        MySQLMetadataReader.isTest = true;
        metadataReader = new MySQLMetadataReader(parameters);
        metadataReader.executeBatch(metadataReader.queryDropTables().concat(metadataReader.queryCreateTables())
        );

    }

    @AfterClass
    public static void clearAfterAllTests() {
        //metadataReader.executeBatch(metadataReader.queryDropTables());
    }

    @Test(expected = RuntimeException.class)
    public void WrongConnectionTest() {
        //given
        String dbType = "MySQL";
        String ipHost = "127.0.0.1";
        String ipPort = "3306";
        String dbName = "test1";
        String dbUser = "root";
        String dbPassword = "root";
        String dbTableName = "study";

        ConnectionParameters parameters = new ConnectionParameters.Builder()
                .addDbType(dbType)
                .addIpHost(ipHost)
                .addIpPort(ipPort)
                .addDbName(dbName)
                .addDbUser(dbUser)
                .addDbPassword(dbPassword)
                .addDbTableName(dbTableName)
                .build();
        //when
        MySQLMetadataReader.isTest = true;
        MySQLMetadataReader metadataReader = new MySQLMetadataReader(parameters);
    }

    @Test(expected = RuntimeException.class)
    public void executeWrongBatchTest() {
        //given
        String query="sql\nals";
        //when
        metadataReader.executeBatch(query);
        //then
    }

    @Test
    public void getTableCommentTest() {
        //given
        String expected = "Table for study";
        //when
        String actual = metadataReader.getTableComment();
        //then
        assertEquals(expected, actual);
    }

    @Test(expected = RuntimeException.class)
    public void getTableCommentOfNonExistingTableTest() {
        //given
        ConnectionParameters parameters = new ConnectionParameters.Builder()
                .addDbType(DB_TYPE)
                .addIpHost(IP_HOST)
                .addIpPort(IP_PORT)
                .addDbName(DB_NAME)
                .addDbUser(DB_USER)
                .addDbPassword(DB_PASSWORD)
                .addDbTableName("not_existing_table")
                .build();
        MySQLMetadataReader metadataReader = new MySQLMetadataReader(parameters);
        String expected = "Table for study";
        //when
        String actual = metadataReader.getTableComment();
        //then
    }

    @Test
    public void getTableColumnsTest() {
        //given
        String expected = "[study_id||int|11|null|null, student_id||int|11|null|1, course_id||int|11|null|1]";
        //when
        String actual = metadataReader.getTableColumnsWithDescription().toString();
        //then
        assertEquals(expected, actual);
    }

    @Test
    public void getTableIndexesTest() {
        //given
        String expected = "[fk_course_id_idx|BTREE|course_id, idx_course_student|BTREE|student_id,course_id, PRIMARY|BTREE|study_id]";
        //when
        String actual = metadataReader.getTableIndexesWithDescription().toString();
        //then
        assertEquals(expected, actual);
    }

    @Test
    public void getTableForeignKeysTest() {
        //given
        String expected = "[fk_course_id|course_id|course|course_id, fk_student_id|student_id|students|student_id]";
        //when
        String actual = metadataReader.getTableForeignKeysWithDescription().toString();
        //then
        assertEquals(expected, actual);
    }
}

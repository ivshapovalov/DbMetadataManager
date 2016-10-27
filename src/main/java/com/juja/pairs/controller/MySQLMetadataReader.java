package com.juja.pairs.controller;

import com.juja.pairs.model.ConnectionParameters;
import com.mysql.jdbc.Driver;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.juja.pairs.DbMetadataManager.logAppender;

public class MySQLMetadataReader extends SQLMetadataReader {
    private final static Logger logger = Logger.getLogger(MySQLMetadataReader.class);
    public static boolean isTest;

    static {
        if (logAppender != null) {
            logger.addAppender(logAppender);
        }
    }

    static {
        try {
            Class.forName("org.mysql.Driver");
        } catch (ClassNotFoundException e) {
            try {
                DriverManager.registerDriver(new Driver());
            } catch (SQLException e1) {
                try {
                    throw new SQLException("Couldn't register driver in case -", e1);
                } catch (SQLException e2) {
                    e2.printStackTrace();
                }
            }
        }
    }

    public MySQLMetadataReader(ConnectionParameters parameters) {
        super(parameters);
        String url = String.format("jdbc:mysql://%s:%s/%s", parameters.getIpHost(), parameters.getIpPort(), parameters.getDbName());
        try {
            connection = DriverManager.getConnection(url, parameters.getDbUser(), parameters.getDbPassword());
        } catch (SQLException e) {
            String message =
                    String.format("Unable to connect to database '%s', user '%s', password '%s'",
                            parameters.getDbName(), parameters.getDbUser(), parameters.getDbPassword());
            if (!isTest) {
                logger.error(message, e);
            }
            throw new RuntimeException(message, e);

        }
    }

    public void executeBatch(String query) {
        String [] batchArray=query.split("[\n]");

        try (Statement stmt = connection.createStatement();) {
            for (String batch:batchArray
                 ) {
                stmt.addBatch(batch);
            }
            stmt.executeBatch();
        } catch (SQLException e) {
            String message = String.format("It is not possible to update");
            if (!isTest) {
                logger.error(message, e);
            }
            throw new RuntimeException(message, e);
        }
    }

    public String getTableComment() {

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(String.format("SELECT TABLE_COMMENT FROM INFORMATION_SCHEMA.Tables " +
                             "WHERE table_name = '%s' AND table_schema = '%s'",
                     parameters.getDbTableName(), parameters.getDbName()))) {
            if (rs.next()) {
                return rs.getString("TABLE_COMMENT");
            }
            return "";
        } catch (SQLException e) {
            String message = String.format("It is not possible to obtain the comment of table '%s' in " +
                    "database '%s'", parameters.getDbTableName(), parameters.getDbName());
            if (!isTest) {
                logger.error(message, e);
            }
            throw new RuntimeException(message, e);
        }
    }

    public List<String> getTableColumnsWithDescription() {
        List<String> columnsWithDescription = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(String.format("Select COLUMN_NAME,COLUMN_COMMENT,COLUMN_TYPE,IS_NULLABLE,COLUMN_DEFAULT " +
                             "FROM INFORMATION_SCHEMA.Columns " +
                             "WHERE table_name = '%s' AND table_schema = '%s'",
                     parameters.getDbTableName(), parameters.getDbName()))) {
            while (rs.next()) {
                StringBuilder line = new StringBuilder();
                line.append(rs.getString("COLUMN_NAME")).append(COLUMN_SEPARATOR);
                line.append(rs.getString("COLUMN_COMMENT")).append(COLUMN_SEPARATOR);
                String columnFullType = rs.getString("COLUMN_TYPE");
                String columnType = columnFullType.substring(0, columnFullType.indexOf("("));
                String columnSize = columnFullType.substring(columnFullType.indexOf("(") + 1, columnFullType.length() - 1);
                line.append(columnType).append(COLUMN_SEPARATOR);
                line.append(columnSize).append(COLUMN_SEPARATOR);
                String nullable = "";
                if (rs.getString("IS_NULLABLE").equalsIgnoreCase("not null")) {
                    nullable = "not null";
                } else {
                    nullable = "null";
                }
                line.append(nullable).append(COLUMN_SEPARATOR);
                line.append(rs.getString("COLUMN_DEFAULT"));
                columnsWithDescription.add(line.toString());
            }
            return columnsWithDescription;
        } catch (SQLException e) {
            String message = String.format("It is not possible to obtain columns of table '%s' in " +
                    "database '%s'", parameters.getDbTableName(), parameters.getDbName());
            if (!isTest) {
                logger.error(message, e);
            }
            throw new RuntimeException(message, e);
        }
    }

    public List<String> getTableIndexesWithDescription() {
        List<String> indexesWithDescription = new ArrayList<>();
        Map<String, String> indexContent = new HashMap<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(String.format("select INDEX_NAME,INDEX_TYPE,COLUMN_NAME,SEQ_IN_INDEX FROM INFORMATION_SCHEMA.STATISTICS " +
                             "WHERE table_name = '%s' AND table_schema = '%s' order by INDEX_NAME ASC, SEQ_IN_INDEX asc",
                     parameters.getDbTableName(), parameters.getDbName()))) {

            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                String indexField = rs.getString("COLUMN_NAME");

                if (indexContent.containsKey(indexName)) {
                    indexContent.put(indexName, indexContent.get(indexName).concat(",").concat(indexField));
                } else {
                    indexContent.put(indexName, indexField);
                }
            }
            rs.beforeFirst();
            while (rs.next()) {
                StringBuilder line = new StringBuilder();
                String indexName = rs.getString("INDEX_NAME");
                if (!indexContent.containsKey(indexName)) {
                    continue;
                }
                line.append(indexName).append(COLUMN_SEPARATOR);
                line.append(rs.getString("INDEX_TYPE")).append(COLUMN_SEPARATOR);
                line.append(indexContent.get(indexName));
                indexesWithDescription.add(line.toString());

                indexContent.remove(indexName);
            }
            return indexesWithDescription;
        } catch (SQLException e) {
            String message = String.format("It is not possible to obtain indexes of table '%s' in " +
                    "database '%s'", parameters.getDbTableName(), parameters.getDbName());
            if (!isTest) {
                logger.error(message, e);
            }
            throw new RuntimeException(message, e);
        }
    }

    public List<String> getTableForeignKeysWithDescription() {
        List<String> foreignKeysWithDescription = new ArrayList<>();
        Map<String, String> foreignKeyContent = new HashMap<>();
        Map<String, String> foreignKeyReferencedContent = new HashMap<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(String.format("" +
                             "SELECT CONSTRAINT_NAME,COLUMN_NAME," +
                             "REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME  FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                             "WHERE table_name = '%s' AND table_schema = '%s' AND CONSTRAINT_NAME<>'PRIMARY'" +
                             "order by CONSTRAINT_NAME,POSITION_IN_UNIQUE_CONSTRAINT",
                     parameters.getDbTableName(), parameters.getDbName()))) {

            while (rs.next()) {
                String fkName = rs.getString("CONSTRAINT_NAME");
                String fkField = rs.getString("COLUMN_NAME");
                String fkReferencedField = rs.getString("REFERENCED_COLUMN_NAME");

                if (foreignKeyContent.containsKey(fkName)) {
                    foreignKeyContent.put(fkName, foreignKeyContent.get(fkName).concat(",").concat(fkField));
                } else {
                    foreignKeyContent.put(fkName, fkField);
                }
                if (foreignKeyReferencedContent.containsKey(fkName)) {
                    foreignKeyReferencedContent.put(fkName, foreignKeyReferencedContent.get(fkName).concat(",").concat(fkReferencedField));
                } else {
                    foreignKeyReferencedContent.put(fkName, fkReferencedField);
                }
            }
            rs.beforeFirst();
            while (rs.next()) {
                StringBuilder line = new StringBuilder();
                String fkName = rs.getString("CONSTRAINT_NAME");
                if (!foreignKeyContent.containsKey(fkName)) {
                    continue;
                }
                line.append(fkName).append(COLUMN_SEPARATOR);
                line.append(foreignKeyContent.get(fkName)).append(COLUMN_SEPARATOR);
                line.append(rs.getString("REFERENCED_TABLE_NAME")).append(COLUMN_SEPARATOR);
                line.append(foreignKeyReferencedContent.get(fkName));

                foreignKeysWithDescription.add(line.toString());
                foreignKeyContent.remove(fkName);
                foreignKeyReferencedContent.remove(fkName);
            }
            return foreignKeysWithDescription;
        } catch (SQLException e) {
            String message = String.format("It is not possible to obtain foreign keys of table '%s' in " +
                    "database '%s'", parameters.getDbTableName(), parameters.getDbName());
            logger.error(message, e);
            throw new RuntimeException(message, e);
        }

    }

    @Override
    public void close() throws IOException {
        try {
            connection.close();
        } catch (SQLException e) {
            String message = String.format("It is not possible to close connection");
            logger.error(message, e);
            throw new RuntimeException(message, e);
        }
    }

    public String queryDropTables() {
        return "DROP TABLE IF exists `test`.`study`;\n" +
                "DROP TABLE IF exists `test`.`course`;\n" +
                "DROP TABLE IF exists `test`.`students`;\n";
    }

    public String queryCreateTables() {

        return  "CREATE TABLE `test`.`students` (" +
                "  `student_id` INT NOT NULL," +
                "  `student_name` VARCHAR(45) NULL," +
                "  `student age` INT(11) NULL," +
                "  PRIMARY KEY (`student_id`)," +
                "  INDEX `student_age` (`student age` DESC)," +
                "  INDEX `student_name` (`student_name` ASC)," +
                "  INDEX `student_name_age` (`student age` ASC, `student_name` DESC))" +
                "COMMENT = 'Table for students';" +
                "\n" +
                "CREATE TABLE `test`.`course` (" +
                "  `course_id` INT NOT NULL," +
                "  `course_name` VARCHAR(45) NULL," +
                "  `course_duration` INT(11) NULL," +
                "  PRIMARY KEY (`course_id`)," +
                "  INDEX `course_name` (`course_name` ASC)," +
                "  INDEX `course_name_duration` (`course_name` ASC, `course_duration` DESC))" +
                "COMMENT = 'Table for cources';" +
                "\n" +
                "CREATE TABLE `test`.`study` (" +
                "  `study_id` INT NOT NULL," +
                "  `student_id` INT(11) NOT NULL DEFAULT 1," +
                "  `course_id` INT(11) NOT NULL DEFAULT 1," +
                "  PRIMARY KEY (`study_id`)," +
                "  INDEX `idx_course_student` (`student_id` ASC, `course_id` ASC)," +
                "  INDEX `fk_course_id_idx` (`course_id` ASC)," +
                "  CONSTRAINT `fk_student_id`" +
                "    FOREIGN KEY (`student_id`)" +
                "    REFERENCES `test`.`students` (`student_id`)" +
                "    ON DELETE NO ACTION" +
                "    ON UPDATE NO ACTION," +
                "  CONSTRAINT `fk_course_id`" +
                "    FOREIGN KEY (`course_id`)" +
                "    REFERENCES `test`.`course` (`course_id`)" +
                "    ON DELETE NO ACTION" +
                "    ON UPDATE NO ACTION)" +
                "COMMENT = 'Table for study';\n";
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.tinkoff.zeppelin.interpreter.content;

import ru.tinkoff.zeppelin.interpreter.InterpreterResult;

import java.io.File;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class H2Manager {
    private Connection connection = null;
    private final H2TableConverter h2TableConverter = new H2TableConverter();

    public void setConnection(final String noteStorePath, final String noteUuid) throws SQLException {
        final String dbLocation = noteStorePath
                + File.separator
                + noteUuid
                + File.separator
                + "outputDB";
        connection = DriverManager.getConnection(
                "jdbc:h2:file:" + Paths.get(dbLocation).normalize().toFile().getAbsolutePath(),
                "sa",
                "");
    }

    public void releaseConnection() throws SQLException {
        if (connection == null) {
            return;
        }
        connection.close();
    }

    public Connection getConnection() {
        return connection;
    }


    public H2TableMetadata getMetadata(final String tableName, final H2TableType type) throws SQLException {

        final String metaQuery = String.format(
                "SELECT ROW_COUNT FROM %s.%s_META;",
                type.getSchemaName(),
                tableName);

        final ResultSet meta = connection.createStatement().executeQuery(metaQuery);
        final long rowNumber = meta.next() ? meta.getLong("ROW_COUNT") : 0L;

        final String tableQuery = String.format("SELECT * FROM %s.%s LIMIT 1;",
                type.getSchemaName(),
                tableName);

        final ResultSet table = connection.createStatement().executeQuery(tableQuery);
        final LinkedHashMap<String, String> columns = h2TableConverter.getColumns(table.getMetaData());
        return new H2TableMetadata(columns, rowNumber);
    }

    public ResultSet getTableNames(final String schema) throws SQLException{
        return connection
                .createStatement()
                .executeQuery(
                        String.format("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
                                        "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME NOT LIKE '%%\\_META'",
                                schema)
                );
    }


    public InterpreterResult.Message saveH2Table(final H2Table h2Table) {

        if (h2Table.getHeader().getName().isEmpty()) {
            return null;
        }

        try {
            final String schema = h2Table.getHeader().getTypeName();
            String tableName = h2Table.getHeader().getName();
            if (schema.equals(H2TableType.SELECT.getSchemaName())) {
                tableName = "_" + updateServiceTableForSelectTable(tableName) + "_" + tableName;
            }
            deleteTable(schema, tableName);
            createTable(schema, tableName, h2Table.getMetadata().getColumns());
            addDataToInsertQuery(insertQuery(schema, tableName, h2Table.getMetadata().getColumns()),
                    h2Table);
            return new InterpreterResult.Message(InterpreterResult.Message.Type.METATAG, schema + "." + tableName);
        } catch (final Exception ex) {
            return new InterpreterResult.Message(InterpreterResult.Message.Type.TEXT, ex.getMessage());
        }
    }


    public void saveMetaTable(final H2Table h2Table){
        if (h2Table.getHeader().getName().isEmpty()){
            return;
        }
        try {
            final String schema = h2Table.getHeader().getTypeName();
            final String table = h2Table.getHeader().getName();
            deleteTable(schema, table + "_META");
            final LinkedHashMap<String, String> metaColumns = saveMetaTable(schema, table);
            addMetaDataToInsertQuery(table,
                    insertQuery(schema, table + "_META", metaColumns),
                    h2Table.getMetadata().getRowCount(),
                    h2Table.getMetadata().getColumns().keySet().size());
        } catch (final Exception ex){
            //SKIP
        }
    }


    private void createTable(final String schemaName,
                             final String tableName,
                             final LinkedHashMap<String, String> columns) throws SQLException {
        connection.createStatement().execute(String.format("CREATE SCHEMA IF NOT EXISTS %s;", schemaName));
        final String createTableScript = buildCreateTableScript(schemaName, tableName, columns);
        connection.createStatement().execute(createTableScript);
    }

    private LinkedHashMap<String, String> saveMetaTable(final String schemaName,
                                                        final String tableName) throws SQLException {

        final LinkedHashMap<String, String> columns = new LinkedHashMap<>();
        columns.put("TABLE_NAME", "VARCHAR(255)");
        columns.put("ROW_COUNT", "BIGINT");
        columns.put("COLUMN_COUNT", "INTEGER");
        final String createTableScript = buildCreateTableScript(schemaName, tableName + "_META", columns);
        connection.createStatement().execute(createTableScript);
        return columns;
    }


    public String deleteTable(final String schema,
                              final String tableName) {
        try {
            connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s.%s;", schema, tableName));
            return String.format("Table %s.%s drop successfully", schema, tableName);
        } catch (final Exception ex) {
            return ex.getMessage();
        }
    }

    private String insertQuery(final String schema,
                               final String tableName,
                               final HashMap<String, String> columns) {
        return String.format(
                "INSERT INTO %s.%s (%s) VALUES (%s);",
                schema,
                tableName,
                String.join(", ", columns.keySet()),
                String.join(",", Collections.nCopies(columns.size(), "?"))
        );
    }

    private void addMetaDataToInsertQuery(final String tableName,
                                          final String preparedQueryScript,
                                          final long rowsCount,
                                          final int columnsCount) {
        try (final PreparedStatement insertValuesStatement = connection.prepareStatement(preparedQueryScript)) {

            insertValuesStatement.setString(1, tableName.toUpperCase());
            insertValuesStatement.setLong(2, rowsCount);
            insertValuesStatement.setInt(3, columnsCount);
            insertValuesStatement.addBatch();
            insertValuesStatement.executeBatch();
        } catch (final Exception ex) {
            //SKIP
        }
    }

    private void addDataToInsertQuery(final String preparedQueryScript,
                                      final H2Table h2Table) {
        try (final PreparedStatement insertValuesStatement = connection.prepareStatement(preparedQueryScript)) {

            for (int i = 0; i < h2Table.getTable().get(0).size(); i++) {
                for(int j = 0; j < h2Table.getTable().size(); j ++){
                    insertValuesStatement.setObject(j + 1, h2Table.getTable().get(j).get(i));
                }
                insertValuesStatement.addBatch();
            }
            insertValuesStatement.executeBatch();

        } catch (final Exception ex) {
            //SKIP
        }
    }


    private String buildCreateTableScript(final String schemaName,
                                          final String tableName,
                                          final LinkedHashMap<String, String> columns) {

        final String payload = columns.entrySet().stream()
                .map(v -> String.format("%s %s", v.getKey(), v.getValue()))
                .collect(Collectors.joining(",\n"));

        return String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s);", schemaName, tableName, payload);
    }

    public ResultSet getTable(final String metatag) {
        try {
            return connection
                    .createStatement()
                    .executeQuery(String.format("SELECT * FROM %s;", metatag));
        } catch (final Exception ex) {
            //skip
            return null;
        }
    }


    private long updateServiceTableForSelectTable(final String tableName) throws SQLException {
        connection.createStatement().execute("CREATE TABLE IF NOT EXISTS PUBLIC.SERVICE_TABLE(" +
                "TABLE_NAME VARCHAR(255) NOT NULL," +
                "PARAM VARCHAR(30) NOT NULL," +
                "VALUE VARCHAR(255) NOT NULL," +
                "UNIQUE(TABLE_NAME, PARAM));");

        //get runCount param in serviceTable
        final ResultSet resultSet = connection.createStatement().executeQuery("SELECT VALUE FROM PUBLIC.SERVICE_TABLE " +
                "WHERE PARAM = 'RUN_COUNT' AND TABLE_NAME = '" + tableName.toUpperCase() + "';");
        long runCount = resultSet == null || !resultSet.next()
                ? 0L
                : resultSet.getLong("VALUE");

        //update runCount param in serviceTable
        if (runCount == 0) {
            connection.createStatement()
                    .execute(String.format("INSERT INTO PUBLIC.SERVICE_TABLE VALUES ('%s', 'RUN_COUNT', %s)",
                            tableName.toUpperCase(),
                            String.valueOf(runCount + 1)));
        } else {
            connection.createStatement()
                    .execute(String.format("UPDATE PUBLIC.SERVICE_TABLE SET VALUE = '%s'" +
                                    " WHERE TABLE_NAME = '%s' AND PARAM = 'RUN_COUNT';",
                            String.valueOf(runCount + 1),
                            tableName.toUpperCase()));//clear previous table if exists and if table_result not pinned

            deleteTable(H2TableType.SELECT.getSchemaName(),
                    String.format("_%s_%s",
                            String.valueOf(runCount),
                            tableName.toUpperCase()));

            deleteTable(H2TableType.SELECT.getSchemaName(),
                    String.format("_%s_%s_META",
                            String.valueOf(runCount),
                            tableName.toUpperCase()));
        }
        //return current runCount param (>=1)
        return ++runCount;
    }
}

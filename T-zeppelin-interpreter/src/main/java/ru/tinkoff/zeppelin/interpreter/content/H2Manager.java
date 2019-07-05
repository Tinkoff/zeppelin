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

import org.apache.commons.lang3.StringUtils;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;

import java.io.File;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class H2Manager {

  public Connection getConnection(final String noteStorePath, final String noteUuid) throws SQLException {
    final String dbLocation = noteStorePath
            + File.separator
            + noteUuid
            + File.separator
            + "outputDB";
    return DriverManager.getConnection(
            "jdbc:h2:file:" + Paths.get(dbLocation).normalize().toFile().getAbsolutePath(),
            "sa",
            "");
  }

  public void releaseConnection(final Connection connection) throws SQLException {
    connection.close();
  }


  public H2TableMetadata getMetadata(final String tableName, final H2TableType type, final Connection connection) throws SQLException {

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
    final Map<String, String> columns = getColumns(table.getMetaData());
    return new H2TableMetadata(columns, rowNumber);
  }


  private LinkedHashMap<String, String> getColumns(final ResultSetMetaData md) throws SQLException {
    final LinkedHashMap<String, String> columns = new LinkedHashMap<>();
    for (int i = 1; i < md.getColumnCount() + 1; i++) {
      columns.put(
              StringUtils.isNotEmpty(md.getColumnLabel(i))
                      ? md.getColumnLabel(i)
                      : md.getColumnName(i),
              md.getColumnTypeName(i)
      );
    }
    return columns;
  }


  public InterpreterResult.Message saveTableAndMetaTable(final String schema,
                                                         String tableName,
                                                         final ResultSet resultSet,
                                                         long rowCount,
                                                         final long rowLimit,
                                                         final Connection h2Connection) {

    if (tableName.isEmpty()) {
      return null;
    }

    try {
      if (schema.equals(H2TableType.SELECT.getSchemaName())) {
        tableName = "_" + addSelectParams(tableName, h2Connection) + "_" + tableName;
      }
      deleteTable(schema, tableName, h2Connection);
      final ResultSetMetaData md = resultSet.getMetaData();
      final LinkedHashMap<String, String> columns = getColumns(md);
      createTable(schema, tableName, columns, h2Connection);
      rowCount = insertValues(h2Connection,
              insertScript(schema, tableName, columns),
              resultSet,
              rowLimit,
              rowCount,
              md.getColumnCount());//columns.keySet().size()


      deleteTable(schema, tableName + "_META", h2Connection);
      final LinkedHashMap<String, String> metacColumns = createMetaTable(schema, tableName, h2Connection);
      metaInsertValues(tableName,
              insertScript(schema, tableName + "_META", metacColumns),
              h2Connection,
              rowCount,
              md.getColumnCount());

      return new InterpreterResult.Message(InterpreterResult.Message.Type.METATAG, schema + "." + tableName);
    } catch (final Exception ex) {
      return new InterpreterResult.Message(InterpreterResult.Message.Type.TEXT, ex.getMessage());
    }
  }


  private void createTable(final String schemaName,
                           final String tableName,
                           final LinkedHashMap<String, String> columns,
                           final Connection connection) throws SQLException {
    connection.createStatement().execute(String.format("CREATE SCHEMA IF NOT EXISTS %s;", schemaName));
    final String createTableScript = buildCreateTableScript(schemaName, tableName, columns);
    connection.createStatement().execute(createTableScript);
  }

  private LinkedHashMap<String, String> createMetaTable(final String schemaName,
                                                        final String tableName,
                                                        final Connection connection) throws SQLException {

    final LinkedHashMap<String, String> columns = new LinkedHashMap<>();
    columns.put("TABLE_NAME", "VARCHAR(255)");
    columns.put("ROW_COUNT", "BIGINT");
    columns.put("COLUMN_COUNT", "INTEGER");
    final String createTableScript = buildCreateTableScript(schemaName, tableName + "_META", columns);
    connection.createStatement().execute(createTableScript);
    return columns;
  }


  public String deleteTable(final String schema,
                            final String tableName,
                            final Connection connection) {
    try {
      connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s.%s;", schema, tableName));
      return String.format("Table %s.%s drop successfully", schema, tableName);
    } catch (final Exception ex) {
      return ex.getMessage();
    }
  }

  private String insertScript(final String schema,
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

  private void metaInsertValues(final String tableName,
                                final String preparedQueryScript,
                                final Connection connection,
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

  private long insertValues(final Connection connection,
                            final String preparedQueryScript,
                            final ResultSet resultSet,
                            final long rowLimit,
                            final long rowCount,
                            final long columnCount) {
    try (final PreparedStatement insertValuesStatement = connection.prepareStatement(preparedQueryScript)) {
      int rowsCount = 0;

      while ((rowLimit == 0 || resultSet.getRow() < rowLimit) && resultSet.next()) {
        for (int i = 1; i <= columnCount; i++) {
          insertValuesStatement.setObject(i, resultSet.getObject(i));
        }
        rowsCount++;
        insertValuesStatement.addBatch();
      }
      insertValuesStatement.executeBatch();

      return rowCount == -1L ? rowsCount : rowCount;
    } catch (final Exception ex) {
      //SKIP
      return 0L;
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


  private long addSelectParams(final String tableName,
                               final Connection connection) throws SQLException {
    //create service table if not exists
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
                      tableName.toUpperCase()),
              connection);

      deleteTable(H2TableType.SELECT.getSchemaName(),
              String.format("_%s_%s_META",
                      String.valueOf(runCount),
                      tableName.toUpperCase()),
              connection);
    }
    //return current runCount param (>=1)
    return ++runCount;
  }
}

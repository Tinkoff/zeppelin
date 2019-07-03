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
package ru.tinkoff.zeppelin.engine;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.content.Content;
import ru.tinkoff.zeppelin.core.content.ContentType;
import ru.tinkoff.zeppelin.storage.ContentDAO;
import ru.tinkoff.zeppelin.storage.ContentParamDAO;
import ru.tinkoff.zeppelin.storage.NoteDAO;

import java.io.File;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

@Component
public class H2MonitoringService {
  private static final Logger LOG = LoggerFactory.getLogger(H2MonitoringService.class);

  private final NoteDAO noteDAO;
  private final ContentDAO contentDAO;
  private final ContentParamDAO contentParamDAO;

  @Autowired
  public H2MonitoringService(final NoteDAO noteDAO,
                             final ContentDAO contentDAO,
                             final ContentParamDAO contentParamDAO) {
    this.noteDAO = noteDAO;
    this.contentDAO = contentDAO;
    this.contentParamDAO = contentParamDAO;
  }

  public void scanH2(final long noteId) {
    final String noteContextPath =
            Configuration.getNoteStorePath()
                    + File.separator
                    + noteDAO.get(noteId).getUuid()
                    + File.separator
                    + "outputDB";
    try (final Connection con = DriverManager.getConnection(
            "jdbc:h2:file:" + Paths.get(noteContextPath).normalize().toFile().getAbsolutePath(),
            "sa",
            "")) {
      compareH2ToContext(con, noteContextPath, noteId);
    } catch (final Throwable th) {
      //Connection error
      LOG.info(th.getMessage());
    }
  }

  private void compareH2ToContext(final Connection connection,
                                  final String locationBase,
                                  final long noteId) throws SQLException, NullPointerException {
    final LinkedList<String> schemas = new LinkedList<>(Arrays.asList("V_TABLES", "R_TABLES"));

    //delete noteId content
    contentDAO.remove(noteId);
    for (final String schema : schemas) {
      //get table names for each schema
      final ResultSet resultSet = connection
              .createStatement()
              .executeQuery(
                      String.format("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
                                      "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME NOT LIKE '%%\\_META'",
                              schema)
              );
      final List<String> tables = new LinkedList<>();
      while (resultSet.next()) {
        tables.add(resultSet.getString("TABLE_NAME"));
      }
      for (final String tableName : tables) {
        //select 1 row from each table in h2 to get metadata
        final ResultSet tableResultSet = connection.createStatement().executeQuery(String.format(
                "SELECT *  FROM %s.%s LIMIT 1;", schema, tableName));
        //create location string = noteStorePath:schema_name.table_name;
        final String location = locationBase + ":" + schema + "." + tableName;

        //create list of columns
        final ResultSetMetaData md = tableResultSet.getMetaData();
        final List<String> columns = new LinkedList<>();
        for (int i = 1; i < md.getColumnCount() + 1; i++) {
          final String createTable = (StringUtils.isNotEmpty(md.getColumnLabel(i))
                  ? md.getColumnLabel(i)
                  : md.getColumnName(i)) + " \t" + md.getColumnTypeName(i);
          columns.add(createTable);
        }

        //get data from table_META table (number of rows, etc.)
        final ResultSet rs = connection.createStatement().executeQuery(String.format(
                "SELECT ROW_COUNT FROM %s.%s_META WHERE TABLE_NAME = '%s';",
                schema,
                tableName,
                tableName.toUpperCase())
        );
        final Long rows = !rs.next()
                ? 0L
                : rs.getLong("ROW_COUNT");

        //add schema.tableName metadata in Content and contentParams tables
        try {
          contentDAO.persist(new Content(noteId, ContentType.TABLE, rows.toString(), location, null));
          final Content content = contentDAO.getContentByLocation(location);
          if (content != null) {
            contentParamDAO.persist(content.getId(),
                    "TABLE_COLUMNS",
                    new Gson().fromJson(new Gson().toJson(columns), JsonElement.class)
            );
          }
        } catch (final Exception ex) {
          //Execute query error
          LOG.info(ex.getMessage());
        }
      }
    }
  }
}

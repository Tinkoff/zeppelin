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
package org.apache.zeppelin.rest;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.NoteContext;

import java.io.File;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/h2")
public class H2RestApi {
  Logger LOGGER = LoggerFactory.getLogger(H2RestApi.class);
  private final Configuration configuration;

  @Autowired
  public H2RestApi(final Configuration configuration) {
    this.configuration = configuration;
  }

  @GetMapping(value = "/get_tables_list/{noteUuid}", produces = "application/json")
  public ResponseEntity getH2SavedTablesList(@PathVariable("noteUuid") final String noteUuid) {


    final String noteContextPath =
            configuration.getNoteStorePath()
                    + File.separator
                    + noteUuid
                    + File.separator
                    + "outputDB";
    LOGGER.info("--------------connection url " + "jdbc:h2:file:" + Paths.get(noteContextPath).normalize().toFile().getAbsolutePath());

    HashMap<String, LinkedList<String>> tables = null;
    final InterpreterResult queryResult;
    Connection con = null;

    try {
      con = DriverManager.getConnection(
              "jdbc:h2:file:" + Paths.get(noteContextPath).normalize().toFile().getAbsolutePath(),
              "sa",
              "");

      LOGGER.info("--------------connection " + con.toString());
      tables = getResultTable(con);
    } catch (final Throwable th) {
      return new JsonResponse(HttpStatus.BAD_REQUEST,
              "Error. Couldn't open connection to internal DB correctly")
              .build();
    } finally {
      if (con != null) {
        try {
          con.close();
        } catch (final Exception e) {
          return new JsonResponse(HttpStatus.BAD_REQUEST,
                  "Error. Couldn't close connection to internal DB correctly")
                  .build();
        }
      }
    }
    return new JsonResponse(HttpStatus.OK, tables).build();
  }


  private HashMap<String, LinkedList<String>> getResultTable(final Connection connection) throws Exception {
    final Statement statement = connection.createStatement();
    statement.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC';");
    final ResultSet resultSet = statement.getResultSet();
    final LinkedList<String> tables = new LinkedList<>();
    while (resultSet.next()) {
      tables.add(resultSet.getString("TABLE_NAME"));
    }

    final HashMap<String, LinkedList<String>> result = new HashMap<>();
    for (String table : tables) {
      result.put(table, new LinkedList<String>());
      final ResultSet tableResultSet = connection.createStatement().executeQuery(
              "SELECT *  FROM " + table + ";");



      final ResultSetMetaData md = tableResultSet.getMetaData();
      for (int i = 1; i < md.getColumnCount() + 1; i++) {
        final StringBuilder createTable = new StringBuilder();
        createTable.append(
                StringUtils.isNotEmpty(md.getColumnLabel(i))
                        ? md.getColumnLabel(i)
                        : md.getColumnName(i)
        );

        createTable.append(" \t");
        createTable.append(md.getColumnTypeName(i));

        result.get(table).add(createTable.toString());
      }

    }
    return result;
  }
}

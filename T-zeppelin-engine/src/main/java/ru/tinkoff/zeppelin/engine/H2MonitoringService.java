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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.content.Content;
import ru.tinkoff.zeppelin.core.content.ContentType;
import ru.tinkoff.zeppelin.interpreter.content.H2Manager;
import ru.tinkoff.zeppelin.interpreter.content.H2TableMetadata;
import ru.tinkoff.zeppelin.interpreter.content.H2TableType;
import ru.tinkoff.zeppelin.storage.ContentDAO;
import ru.tinkoff.zeppelin.storage.ContentParamDAO;
import ru.tinkoff.zeppelin.storage.NoteDAO;
import java.io.File;
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
  private final H2Manager h2Manager = new H2Manager();

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
    try {
      h2Manager.setConnection(Configuration.getNoteStorePath(), noteDAO.get(noteId).getUuid());
      compareH2ToContext(noteContextPath, noteId);
      h2Manager.releaseConnection();
    } catch (final Throwable th) {
      //Connection error
      LOG.info(th.getMessage());
    }
  }

  private void compareH2ToContext(final String locationBase,
                                  final long noteId) throws SQLException, NullPointerException {
    final LinkedList<String> schemas = new LinkedList<>(Arrays.asList("V_TABLES", "R_TABLES", "S_TABLES"));

    contentDAO.remove(noteId);

    for (final String schema : schemas) {
      final ResultSet resultSet = h2Manager.getTableNames(schema);
      final List<String> tables = new LinkedList<>();
      while (resultSet.next()) {
        tables.add(resultSet.getString("TABLE_NAME"));
      }

      for (final String tableName : tables) {
        final String location = locationBase + ":" + schema + "." + tableName;
        final H2TableMetadata h2TableMetadata = h2Manager.getMetadata(tableName, H2TableType.valueOf(schema));

        try {
          contentDAO.persist(new Content(noteId, ContentType.TABLE, String.valueOf(h2TableMetadata.getRowCount()), location, null));
          final Content content = contentDAO.getContentByLocation(location);
          if (content != null) {
            contentParamDAO.persist(content.getId(),
                    "TABLE_COLUMNS",
                    new Gson().fromJson(new Gson().toJson(h2TableMetadata.getColumns()), JsonElement.class)
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

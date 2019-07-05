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
package org.apache.zeppelin.websocket.dto;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.externalDTO.InterpreterResultDTO;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.content.H2Manager;
import ru.tinkoff.zeppelin.interpreter.content.H2TableMetadata;
import ru.tinkoff.zeppelin.interpreter.content.H2TableType;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.LinkedList;
import java.util.List;

@Component
public class MetatagToTableConverter {
  private static final Logger LOG = LoggerFactory.getLogger(MetatagToTableConverter.class);
  private final H2Manager h2Manager = new H2Manager();


  public List<InterpreterResultDTO.Message> scanH2(final String noteUuid, final InterpreterResultDTO.Message metatag) {
    final List<InterpreterResultDTO.Message> msg = new LinkedList<>();
      try (final Connection con = h2Manager.getConnection(Configuration.getNoteStorePath(), noteUuid)) {
        msg.addAll(loadResultTableFromH2(con, metatag.getData()));
      } catch (final Throwable th) {
        //Connection error
        msg.add(new InterpreterResultDTO.Message("TEXT", "Error can't connect to h2 database"));
      }
    return msg;
  }



  private List<InterpreterResultDTO.Message> loadResultTableFromH2(final Connection connection,
                                                                final String metatag) {
    final List<InterpreterResultDTO.Message> messages = new LinkedList<>();
    try {
      final H2TableType h2TableType = H2TableType.fromSchemaName(metatag.split("\\.")[0]);
      final String tableName = metatag.split("\\.")[1];

      final ResultSet resultSet = connection
              .createStatement()
              .executeQuery(String.format("SELECT * FROM %s;", metatag));

      if (h2TableType != H2TableType.SELECT) {
       try {
        final H2TableMetadata h2TableMetadata = h2Manager.getMetadata(tableName, h2TableType, connection);
          messages.add(new InterpreterResultDTO.Message(InterpreterResult.Message.Type.TEXT.name(),
                  String.format("Table %s successfully created in H2 database\n%s%s rows affected",
                          metatag,
                          h2TableMetadata.getColumnsString(),
                          h2TableMetadata.getRowCount())));
        } catch (final Exception ex) {
          messages.add(new InterpreterResultDTO.Message(InterpreterResult.Message.Type.TEXT.name(),
                  "Can't get metadata from h2 database"));
          LOG.info(ex.getMessage());
        }
      }
      if (h2TableType != H2TableType.VIRTUAL){
        messages.add(new InterpreterResultDTO.Message(InterpreterResult.Message.Type.TABLE.name(), wrapResults(resultSet)));
      }
    } catch (final Exception ex) {
      messages.add(new InterpreterResultDTO.Message(InterpreterResult.Message.Type.TEXT.name(),
              "Error on load result from H2"));
      LOG.info(ex.getMessage());
    }
    return messages;
  }

  /**
   * Wraps result set to table.
   *
   * @param resultSet - result set to convert.
   * @return converted result, {@code null} if process failed.
   */
  @Nullable
  private String wrapResults(final ResultSet resultSet) {
    try {
      final ResultSetMetaData md = resultSet.getMetaData();
      final StringBuilder msg = new StringBuilder();

      for (int i = 1; i < md.getColumnCount() + 1; i++) {
        if (i > 1) {
          msg.append('\t');
        }
        if (md.getColumnLabel(i) != null && !md.getColumnLabel(i).equals("")) {
          msg.append(replaceReservedChars(md.getColumnLabel(i)));
        } else {
          msg.append(replaceReservedChars(md.getColumnName(i)));
        }
      }
      msg.append('\n');

      while (resultSet.next()) {
        for (int i = 1; i < md.getColumnCount() + 1; i++) {
          final Object resultObject;
          final String resultValue;
          resultObject = resultSet.getObject(i);
          if (resultObject == null) {
            resultValue = "null";
          } else {
            resultValue = resultSet.getString(i);
          }
          msg.append(replaceReservedChars(resultValue));
          if (i != md.getColumnCount()) {
            msg.append('\t');
          }
        }
        msg.append('\n');
      }
      return msg.toString();
    } catch (final Exception e) {
      // LOGGER.error("Failed to parse result", e);
      return null;
    }
  }

  /**
   * For table response replace Tab and Newline characters from the content.
   */
  private String replaceReservedChars(final String str) {
    if (str == null) {
      return "";
    }
    return str.replace('\t', ' ')
            .replace('\n', ' ');
  }
}

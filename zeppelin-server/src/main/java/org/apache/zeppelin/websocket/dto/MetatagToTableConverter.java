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
import ru.tinkoff.zeppelin.interpreter.content.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Component
public class MetatagToTableConverter {
  private static final Logger LOG = LoggerFactory.getLogger(MetatagToTableConverter.class);
  private final H2Manager h2Manager = new H2Manager();
  private final H2TableConverter h2TableConverter = new H2TableConverter();


  List<InterpreterResultDTO.Message> scanH2(final String noteUuid,
                                            final InterpreterResultDTO.Message metaTag) {
    final List<InterpreterResultDTO.Message> msg = new LinkedList<>();
    try {
      h2Manager.setConnection(Configuration.getNoteStorePath(), noteUuid);
      msg.addAll(loadResultTableFromH2(metaTag.getData()));
      h2Manager.releaseConnection();
    } catch (
            final Throwable th) {
      //Connection error
      msg.add(new InterpreterResultDTO.Message("TEXT", "Error can't connect to h2 database"));
    } finally {
      try {
        h2Manager.releaseConnection();
      } catch (final Exception e) {
        //SKIP
      }
    }
    return msg;
  }


  private List<InterpreterResultDTO.Message> loadResultTableFromH2(final String metatag) {
    final List<InterpreterResultDTO.Message> messages = new LinkedList<>();
    try {
      final H2TableType h2TableType = H2TableType.fromSchemaName(metatag.split("\\.")[0]);
      final String tableName = metatag.split("\\.")[1];

      if (h2TableType != H2TableType.SELECT) {
        try {
          final H2TableMetadata h2TableMetadata = h2Manager.getMetadata(tableName, h2TableType);
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
      if (h2TableType != H2TableType.VIRTUAL) {
        messages.add(new InterpreterResultDTO.Message(InterpreterResult.Message.Type.TABLE.name(),
                wrapH2Table(h2TableConverter.resultSetToTable(h2Manager.getTable(metatag),
                        tableName,
                        h2TableType,
                        -1,
                        0))
        ));
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
   * @param table - H2Table to convert.
   * @return converted result, {@code null} if process failed.
   */
  private String wrapH2Table(final H2Table table) {
    try {
      final H2TableMetadata md = table.getMetadata();
      final StringBuilder msg = new StringBuilder();

      for (final String s : md.getColumns().keySet()) {
        msg.append(replaceReservedChars(s)).append("\t");
      }
      msg.delete(msg.lastIndexOf("\t"), msg.length());
      msg.append('\n');

      for (int i = 0; i < table.getTable().get(0).size(); i++) {
        for (final ArrayList<Object> li : table.getTable()) {
          final String resultValue;
          final Object resultObject = li.get(i);
          if (resultObject == null) {
            resultValue = "null";
          } else {
            resultValue = resultObject.toString();
          }
          msg.append(replaceReservedChars(resultValue));
          msg.append('\t');
        }
        msg.delete(msg.lastIndexOf("\t"), msg.length());
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

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
package ru.tinkoff.zeppelin.storage;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.externalDTO.InterpreterResultDTO;
import ru.tinkoff.zeppelin.core.externalDTO.ParagraphDTO;

@Component
public class FullParagraphDAO {

  private final static String GET_BY_PARAGRAPH_ID = "" +
          "SELECT P.ID,\n" +
          "       P.UUID,\n" +
          "       P.TITLE,\n" +
          "       P.TEXT,\n" +
          "       P.SHEBANG,\n" +
          "       P.CREATED,\n" +
          "       P.UPDATED,\n" +
          "       P.POSITION,\n" +
          "       J.ID AS JOB_ID,\n" +
          "       J.STATUS,\n" +
          "       J.STARTED_AT,\n" +
          "       J.ENDED_AT,\n" +
          "       J.USER_NAME\n" +
          "FROM PARAGRAPHS P\n" +
          "       LEFT JOIN JOB J ON P.JOB_ID = J.ID\n" +
          "WHERE P.ID = :ID;";

  private final static String GET_BY_PARAGRAPH_UUID = "" +
          "SELECT P.ID,\n" +
          "       P.UUID,\n" +
          "       P.TITLE,\n" +
          "       P.TEXT,\n" +
          "       P.SHEBANG,\n" +
          "       P.CREATED,\n" +
          "       P.UPDATED,\n" +
          "       P.POSITION,\n" +
          "       J.ID AS JOB_ID,\n" +
          "       J.STATUS,\n" +
          "       J.STARTED_AT,\n" +
          "       J.ENDED_AT,\n" +
          "       J.USER_NAME\n" +
          "FROM PARAGRAPHS P\n" +
          "       LEFT JOIN JOB J ON P.JOB_ID = J.ID\n" +
          "WHERE P.UUID = :UUID;";

  private final static String GET_BY_NOTE_ID = "" +
          "SELECT P.ID,\n" +
          "       P.UUID,\n" +
          "       P.TITLE,\n" +
          "       P.TEXT,\n" +
          "       P.SHEBANG,\n" +
          "       P.CREATED,\n" +
          "       P.UPDATED,\n" +
          "       P.POSITION,\n" +
          "       J.ID AS JOB_ID,\n" +
          "       J.STATUS,\n" +
          "       J.STARTED_AT,\n" +
          "       J.ENDED_AT,\n" +
          "       J.USER_NAME\n" +
          "FROM NOTES N\n" +
          "       LEFT JOIN PARAGRAPHS P ON N.ID = P.NOTE_ID\n" +
          "       LEFT JOIN JOB J ON P.JOB_ID = J.ID\n" +
          "WHERE N.ID = :ID;";

  private final static String GET_BY_NOTE_UUID = "" +
          "SELECT P.ID,\n" +
          "       P.UUID,\n" +
          "       P.TITLE,\n" +
          "       P.TEXT,\n" +
          "       P.SHEBANG,\n" +
          "       P.CREATED,\n" +
          "       P.UPDATED,\n" +
          "       P.POSITION,\n" +
          "       J.ID AS JOB_ID,\n" +
          "       J.STATUS,\n" +
          "       J.STARTED_AT,\n" +
          "       J.ENDED_AT,\n" +
          "       J.USER_NAME\n" +
          "FROM NOTES N\n" +
          "       LEFT JOIN PARAGRAPHS P ON N.ID = P.NOTE_ID\n" +
          "       LEFT JOIN JOB J ON P.JOB_ID = J.ID\n" +
          "WHERE N.UUID = :UUID;";

  private static final String LOAD_PAYLOAD_BY_JOB_ID = "SELECT J.STATUS,\n" +
          "       JR.TYPE,\n" +
          "       JR.RESULT\n" +
          "FROM JOB J LEFT JOIN JOB_RESULT JR ON J.ID = JR.JOB_ID\n" +
          "WHERE J.ID = :ID;\n";


  private final NamedParameterJdbcTemplate jdbcTemplate;
  private final static Gson gson = new Gson();

  public FullParagraphDAO(final NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  private static InterpreterResultDTO.Message mapRowResult(ResultSet resultSet, int i) throws SQLException {
    final String type = resultSet.getString("TYPE");
    final String result = resultSet.getString("RESULT");

    return new InterpreterResultDTO.Message(
            type,
            result
    );
  }

  private static ParagraphDTO mapRow(final ResultSet resultSet, final int i) throws SQLException {

    final Type configType = new TypeToken<Map<String, Object>>() {
    }.getType();

    final Long id = resultSet.getLong("ID");
    final Long jobId = resultSet.getLong("JOB_ID");
    final String uuid = resultSet.getString("UUID");
    final String title = resultSet.getString("TITLE");
    final String text = resultSet.getString("TEXT");
    final String shebang = resultSet.getString("SHEBANG");
    final LocalDateTime created =
            null != resultSet.getTimestamp("CREATED")
                    ? resultSet.getTimestamp("CREATED").toLocalDateTime()
                    : null;
    final LocalDateTime updated =
            null != resultSet.getTimestamp("UPDATED")
                    ? resultSet.getTimestamp("UPDATED").toLocalDateTime()
                    : null;

    final LocalDateTime startedAt =
            null != resultSet.getTimestamp("STARTED_AT")
                    ? resultSet.getTimestamp("STARTED_AT").toLocalDateTime()
                    : null;

    final LocalDateTime endedAt =
            null != resultSet.getTimestamp("ENDED_AT")
                    ? resultSet.getTimestamp("ENDED_AT").toLocalDateTime()
                    : null;


    final String status = resultSet.getString("STATUS");
    final Integer position = resultSet.getInt("POSITION");

    final String user = resultSet.getString("USER_NAME");
    final Map<String, Object> config = new HashMap<>();
    final Map<String, Object> formParams = new HashMap<>();

    final ParagraphDTO paragraphDTO = new ParagraphDTO();
    paragraphDTO.setJobId(jobId);
    paragraphDTO.setDatabaseId(id);
    paragraphDTO.setId(uuid);
    paragraphDTO.setTitle(title);
    paragraphDTO.setText(text);
    paragraphDTO.setUser(user);
    paragraphDTO.setShebang(shebang);
    paragraphDTO.setCreated(created);
    paragraphDTO.setUpdated(updated);
    paragraphDTO.setStatus(status);
    paragraphDTO.setConfig(config);
    paragraphDTO.setFormParams(formParams);
    paragraphDTO.setPosition(position);
    paragraphDTO.setStartedAt(startedAt);
    paragraphDTO.setEndedAt(endedAt);
    return paragraphDTO;

  }

  public ParagraphDTO getById(final Long id) {
    SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", id);

    final ParagraphDTO paragraphDTO = jdbcTemplate.query(
            GET_BY_PARAGRAPH_ID,
            parameters,
            FullParagraphDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);

    final List<InterpreterResultDTO.Message> resultDTOS = new ArrayList<>();
    if (paragraphDTO != null && paragraphDTO.getJobId() != 0) {
      parameters = new MapSqlParameterSource()
              .addValue("ID", paragraphDTO.getJobId());

      jdbcTemplate.query(
              LOAD_PAYLOAD_BY_JOB_ID,
              parameters,
              FullParagraphDAO::mapRowResult)
              .stream()
              .filter(message -> message.getType() != null)
              .forEach(resultDTOS::add);

      if (resultDTOS.isEmpty()) {
        resultDTOS.add(new InterpreterResultDTO.Message("TEXT", " "));
      }
      paragraphDTO.setResults(new InterpreterResultDTO(paragraphDTO.getStatus(), resultDTOS));
    } else if (paragraphDTO != null) {

      resultDTOS.add(new InterpreterResultDTO.Message("TEXT", " "));
      paragraphDTO.setResults(new InterpreterResultDTO(paragraphDTO.getStatus(), resultDTOS));
    }
    return paragraphDTO;
  }
}

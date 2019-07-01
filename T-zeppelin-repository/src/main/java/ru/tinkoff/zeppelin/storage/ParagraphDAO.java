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
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;

import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static ru.tinkoff.zeppelin.storage.Utils.generatePGjson;
import static ru.tinkoff.zeppelin.storage.Utils.toTimestamp;

@Component
public class ParagraphDAO {

  private final static String PERSIST_PARAGRAPH = "" +
          "INSERT INTO PARAGRAPHS (NOTE_ID,\n" +
          "                        UUID,\n" +
          "                        TITLE,\n" +
          "                        TEXT,\n" +
          "                        SHEBANG,\n" +
          "                        CREATED,\n" +
          "                        UPDATED,\n" +
          "                        POSITION,\n" +
          "                        JOB_ID,\n" +
          "                        CONFIG,\n" +
          "                        FORM_PARAMS,\n" +
          "                        REVISION_ID)\n" +
          "VALUES (:NOTE_ID,\n" +
          "        :UUID,\n" +
          "        :TITLE,\n" +
          "        :TEXT,\n" +
          "        :SHEBANG,\n" +
          "        :CREATED,\n" +
          "        :UPDATED,\n" +
          "        :POSITION,\n" +
          "        :JOB_ID,\n" +
          "        :CONFIG,\n" +
          "        :FORM_PARAMS,\n" +
          "        :REVISION_ID);";

  private final static String UPDATE_PARAGRAPH = "" +
          "UPDATE PARAGRAPHS\n" +
          "SET NOTE_ID     = :NOTE_ID,\n" +
          "    UUID        = :UUID,\n" +
          "    TITLE       = :TITLE,\n" +
          "    TEXT        = :TEXT,\n" +
          "    SHEBANG     = :SHEBANG,\n" +
          "    CREATED     = :CREATED,\n" +
          "    UPDATED     = :UPDATED,\n" +
          "    POSITION    = :POSITION,\n" +
          "    JOB_ID      = :JOB_ID,\n" +
          "    CONFIG      = :CONFIG,\n" +
          "    FORM_PARAMS = :FORM_PARAMS,\n" +
          "    REVISION_ID = :REVISION_ID\n" +
          "WHERE ID = :ID;";


  private final static String DELETE_PARAGRAPH = "" +
          "DELETE\n" +
          "FROM PARAGRAPHS\n" +
          "WHERE ID = :ID;";

  private final static String SELECT_PARAGRAPHS = "" +
          "SELECT ID,\n" +
          "       NOTE_ID,\n" +
          "       UUID,\n" +
          "       TITLE,\n" +
          "       TEXT,\n" +
          "       SHEBANG,\n" +
          "       CREATED,\n" +
          "       UPDATED,\n" +
          "       POSITION,\n" +
          "       JOB_ID,\n" +
          "       CONFIG,\n" +
          "       FORM_PARAMS,\n" +
          "       REVISION_ID\n" +
          "FROM PARAGRAPHS\n" +
          "WHERE REVISION_ID ISNULL;";

  private final static String SELECT_PARAGRAPH_BY_ID = "" +
          "SELECT ID,\n" +
          "       NOTE_ID,\n" +
          "       UUID,\n" +
          "       TITLE,\n" +
          "       TEXT,\n" +
          "       SHEBANG,\n" +
          "       CREATED,\n" +
          "       UPDATED,\n" +
          "       POSITION,\n" +
          "       JOB_ID,\n" +
          "       CONFIG,\n" +
          "       FORM_PARAMS,\n" +
          "       REVISION_ID\n" +
          "FROM PARAGRAPHS\n" +
          "  WHERE ID = :ID;";

  private final static String SELECT_PARAGRAPH_BY_UUID = "" +
          "SELECT ID,\n" +
          "       NOTE_ID,\n" +
          "       UUID,\n" +
          "       TITLE,\n" +
          "       TEXT,\n" +
          "       SHEBANG,\n" +
          "       CREATED,\n" +
          "       UPDATED,\n" +
          "       POSITION,\n" +
          "       JOB_ID,\n" +
          "       CONFIG,\n" +
          "       FORM_PARAMS,\n" +
          "       REVISION_ID\n" +
          "FROM PARAGRAPHS\n" +
          "  WHERE UUID = :UUID;";

  private final static String SELECT_PARAGRAPH_BY_NOTE_ID = "" +
          "SELECT ID,\n" +
          "       NOTE_ID,\n" +
          "       UUID,\n" +
          "       TITLE,\n" +
          "       TEXT,\n" +
          "       SHEBANG,\n" +
          "       CREATED,\n" +
          "       UPDATED,\n" +
          "       POSITION,\n" +
          "       JOB_ID,\n" +
          "       CONFIG,\n" +
          "       FORM_PARAMS,\n" +
          "       REVISION_ID\n" +
          "FROM PARAGRAPHS\n" +
          "WHERE NOTE_ID = :NOTE_ID\n" +
          "  AND REVISION_ID ISNULL\n" +
          "ORDER BY POSITION;";

  private final static String SELECT_PARAGRAPH_BY_REVISION_ID = "" +
          "SELECT ID,\n" +
          "       NOTE_ID,\n" +
          "       UUID,\n" +
          "       TITLE,\n" +
          "       TEXT,\n" +
          "       SHEBANG,\n" +
          "       CREATED,\n" +
          "       UPDATED,\n" +
          "       POSITION,\n" +
          "       JOB_ID,\n" +
          "       CONFIG,\n" +
          "       FORM_PARAMS,\n" +
          "       REVISION_ID\n" +
          "FROM PARAGRAPHS\n" +
          "WHERE REVISION_ID = :REVISION_ID\n" +
          "ORDER BY POSITION;";


  private final NamedParameterJdbcTemplate jdbcTemplate;
  private final static Gson gson = new Gson();

  public ParagraphDAO(final NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  private static Paragraph mapRow(final ResultSet resultSet, final int i) throws SQLException {

    final Type configType = new TypeToken<Map<String, Object>>() {}.getType();

    final Long id = resultSet.getLong("ID");
    final Long noteId = resultSet.getLong("NOTE_ID");
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
    final Integer position = resultSet.getInt("POSITION");
    final Long jobId =  null != resultSet.getString("JOB_ID")
            ? resultSet.getLong("JOB_ID")
            : null;

    final Map<String, Object> config = gson.fromJson(resultSet.getString("CONFIG"), configType);
    final Map<String, Object> formParams = gson.fromJson(resultSet.getString("FORM_PARAMS"), configType);
    final Long revisionId =  null != resultSet.getString("REVISION_ID")
            ? resultSet.getLong("REVISION_ID")
            : null;

    return new Paragraph(
            id,
            noteId,
            uuid,
            title,
            text,
            shebang,
            created,
            updated,
            position,
            jobId,
            revisionId,
            config,
            formParams);
  }

  public Paragraph persist(final Paragraph paragraph) {
    final KeyHolder holder = new GeneratedKeyHolder();
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("NOTE_ID", paragraph.getNoteId())
            .addValue("UUID", paragraph.getUuid())
            .addValue("TITLE", paragraph.getTitle())
            .addValue("TEXT", paragraph.getText())
            .addValue("SHEBANG", paragraph.getShebang())
            .addValue("CREATED", toTimestamp(paragraph.getCreated()))
            .addValue("UPDATED", toTimestamp(paragraph.getUpdated()))
            .addValue("POSITION", paragraph.getPosition())
            .addValue("JOB_ID", paragraph.getJobId())
            .addValue("CONFIG", generatePGjson(paragraph.getConfig()))
            .addValue("FORM_PARAMS", generatePGjson(paragraph.getFormParams()))
            .addValue("REVISION_ID", paragraph.getRevisionId());
    jdbcTemplate.update(PERSIST_PARAGRAPH, parameters, holder);

    paragraph.setId((Long) holder.getKeys().get("id"));
    return paragraph;
  }

  public Paragraph update(final Paragraph paragraph) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("NOTE_ID", paragraph.getNoteId())
            .addValue("UUID", paragraph.getUuid())
            .addValue("TITLE", paragraph.getTitle())
            .addValue("TEXT", paragraph.getText())
            .addValue("SHEBANG", paragraph.getShebang())
            .addValue("CREATED", toTimestamp(paragraph.getCreated()))
            .addValue("UPDATED", toTimestamp(paragraph.getUpdated()))
            .addValue("POSITION", paragraph.getPosition())
            .addValue("JOB_ID", paragraph.getJobId())
            .addValue("CONFIG", generatePGjson(paragraph.getConfig()))
            .addValue("FORM_PARAMS", generatePGjson(paragraph.getFormParams()))
            .addValue("REVISION_ID", paragraph.getRevisionId())
            .addValue("ID", paragraph.getId());
    jdbcTemplate.update(UPDATE_PARAGRAPH, parameters);
    return paragraph;
  }

  public List<Paragraph> getAll() {
    final SqlParameterSource parameters = new MapSqlParameterSource();

    return jdbcTemplate.query(
            SELECT_PARAGRAPHS,
            parameters,
            ParagraphDAO::mapRow);
  }

  public Paragraph get(final Long id) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", id);

    return jdbcTemplate.query(
            SELECT_PARAGRAPH_BY_ID,
            parameters,
            ParagraphDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public Paragraph get(final String uuid) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("UUID", uuid);

    return jdbcTemplate.query(
            SELECT_PARAGRAPH_BY_UUID,
            parameters,
            ParagraphDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public void remove(final Paragraph paragraph) {
    jdbcTemplate.update(DELETE_PARAGRAPH, new MapSqlParameterSource("ID", paragraph.getId()));
  }

  public List<Paragraph> getByNoteId(final Long noteId) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("NOTE_ID", noteId);
    return jdbcTemplate.query(
            SELECT_PARAGRAPH_BY_NOTE_ID,
            parameters,
            ParagraphDAO::mapRow);
  }

  public List<Paragraph> getByRevisionId(final Long revisionId) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("REVISION_ID", revisionId);
    return jdbcTemplate.query(
            SELECT_PARAGRAPH_BY_REVISION_ID,
            parameters,
            ParagraphDAO::mapRow);
  }
}
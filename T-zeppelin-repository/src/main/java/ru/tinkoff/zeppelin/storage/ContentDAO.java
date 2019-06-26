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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.content.Content;
import ru.tinkoff.zeppelin.core.content.ContentType;
import ru.tinkoff.zeppelin.core.notebook.Note;

@Component
public class ContentDAO {

  private static final String GET_NOTE_CONTENT = "" +
      "SELECT ID,\n" +
      "       TYPE,\n" +
      "       DESCRIPTION,\n" +
      "       LOCATION\n" +
      "FROM CONTENT\n" +
      "WHERE NOTE_ID = :NOTE_ID;";

  private static final String GET_CONTENT = "" +
      "SELECT NOTE_ID,\n" +
      "       TYPE,\n" +
      "       DESCRIPTION,\n" +
      "       LOCATION\n" +
      "FROM CONTENT\n" +
      "WHERE ID = :ID;";

  private static final String GET_CONTENT_BY_LOCATION = "" +
      "SELECT ID,\n" +
      "       NOTE_ID,\n" +
      "       TYPE,\n" +
      "       DESCRIPTION\n" +
      "FROM CONTENT\n" +
      "WHERE LOCATION = :LOCATION;";

  private static final String ADD_CONTENT =  "" +
      "INSERT INTO CONTENT (NOTE_ID,\n" +
      "                     TYPE,\n" +
      "                     DESCRIPTION,\n" +
      "                     LOCATION)\n" +
      "VALUES (:NOTE_ID,\n" +
      "        :TYPE,\n" +
      "        :DESCRIPTION,\n" +
      "        :LOCATION);";

  private static final String DELETE = "" +
      "DELETE\n" +
      "FROM CONTENT\n" +
      "WHERE ID = :ID;";

  private static final String UPDATE = "" +
      "UPDATE CONTENT\n" +
      "SET NOTE_ID                  = :NOTE_ID,\n" +
      "    TYPE                     = :TYPE,\n" +
      "    DESCRIPTION              = :DESCRIPTION,\n" +
      "    LOCATION                 = :LOCATION\n" +
      "WHERE ID = :ID;";

  private final NamedParameterJdbcTemplate jdbcTemplate;


  public ContentDAO(final NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  private static Content mapRow(final ResultSet resultSet, final int i) throws SQLException {
    final Long id = resultSet.getLong("ID");
    final Long noteId = resultSet.getLong("NOTE_ID");
    final String description = resultSet.getString("DESCRIPTION");
    final String location = resultSet.getString("LOCATION");
    final ContentType contentType = ContentType.valueOf(resultSet.getString("TYPE"));
    return new Content(id, noteId, contentType, description, location);
  }

  public Content persist(@Nonnull final Content content) {
    final KeyHolder holder = new GeneratedKeyHolder();
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("NOTE_ID", content.getNoteId())
        .addValue("TYPE", content.getType())
        .addValue("DESCRIPTION", content.getDescription())
        .addValue("LOCATION", content.getLocation());

    jdbcTemplate.update(ADD_CONTENT, parameters, holder);
    content.setDatabaseId((Long) holder.getKeys().get("ID"));
    return content;
  }

  public Content update(@Nonnull final Content content) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("NOTE_ID", content.getNoteId())
        .addValue("TYPE", content.getType())
        .addValue("DESCRIPTION", content.getDescription())
        .addValue("LOCATION", content.getLocation())
        .addValue("ID", content.getDatabaseId());

    jdbcTemplate.update(UPDATE, parameters);
    return content;
  }

  public void remove(@Nonnull final Content content) {
    jdbcTemplate.update(DELETE, new MapSqlParameterSource("ID", content.getDatabaseId()));
  }

  @Nonnull
  public List<Content> getNoteContent(@Nonnull final Note note) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("NOTE_ID", note.getId());

    return jdbcTemplate.query(
        GET_NOTE_CONTENT,
        parameters,
        ContentDAO::mapRow
    );
  }

  @Nullable
  public Content getContentByLocation(@Nonnull final String location) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("LOCATION", location);

    return jdbcTemplate.query(
        GET_CONTENT_BY_LOCATION,
        parameters,
        ContentDAO::mapRow)
        .stream()
        .findFirst()
        .orElse(null);
  }

  @Nullable
  public Content getContent(final long databaseId) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("ID", databaseId);

    return jdbcTemplate.query(
        GET_CONTENT,
        parameters,
        ContentDAO::mapRow)
        .stream()
        .findFirst()
        .orElse(null);
  }
}

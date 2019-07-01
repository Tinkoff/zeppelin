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
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.NoteRevision;

import static ru.tinkoff.zeppelin.storage.Utils.toTimestamp;

@Component
public class NoteRevisionDAO {

  private final NamedParameterJdbcTemplate jdbcTemplate;

  private static final String INSERT_REVISION =
      "INSERT INTO revisions (note_id, message, date) \n "
          + "values (:note_id, :message, :date);";

  private static final String GET_ALL_REVISIONS =
      "SELECT * \n"
          + "FROM revisions \n"
          + "WHERE note_id=:note_id;";


  @Autowired
  public NoteRevisionDAO(final NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public NoteRevision createRevision(final Note note, final String message) {

    if (note.getRevision() != null) {
      throw new RuntimeException("Can't create revision. The note is in view of another revision.");
    }

    NoteRevision revision = new NoteRevision(note.getId(), LocalDateTime.now(), message);

    GeneratedKeyHolder holder = new GeneratedKeyHolder();
    jdbcTemplate.update(INSERT_REVISION, getParameters(revision), holder);
    revision.setId((Long) holder.getKeys().get("id"));

    return revision;
  }

  public List<NoteRevision> getRevisions(final long noteId) {
    return jdbcTemplate.query(
        GET_ALL_REVISIONS,
        new MapSqlParameterSource("note_id", noteId),
        this::mapRow
    );
  }

  private NoteRevision mapRow(final ResultSet resultSet, final int i) throws SQLException {
    long id = resultSet.getLong("id");
    long noteId = resultSet.getLong("note_id");
    String message = resultSet.getString("message");
    LocalDateTime date = resultSet.getTimestamp("date").toLocalDateTime();

    NoteRevision revision = new NoteRevision(noteId, date, message);
    revision.setId(id);
    return revision;
  }

  private MapSqlParameterSource getParameters(final NoteRevision revision) {
    return new MapSqlParameterSource()
        .addValue("note_id", revision.getNoteId())
        .addValue("message", revision.getMessage())
        .addValue("date", toTimestamp(revision.getDate()));
  }
}

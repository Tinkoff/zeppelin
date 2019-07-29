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

import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Note.NoteViewMode;

@Component
public class NoteDAO {
  private static final String PERSIST_NOTE = "" +
          "INSERT INTO NOTES (UUID,\n" +
          "                   PATH,\n" +
          "                   JOB_BATCH_ID,\n" +
          "                   VIEW_MODE)\n" +
          "VALUES (:UUID,\n" +
          "        :PATH,\n" +
          "        :JOB_BATCH_ID,\n" +
          "        :VIEW_MODE);";

  private static final String UPDATE_NOTE = "" +
          "UPDATE NOTES\n" +
          "SET UUID         = :UUID,\n" +
          "    PATH         = :PATH,\n" +
          "    JOB_BATCH_ID = :JOB_BATCH_ID,\n" +
          "    VIEW_MODE    = :VIEW_MODE\n" +
          "WHERE ID = :ID;";


  private static final String GET_NOTE_BY_ID = "" +
          "SELECT ID,\n" +
          "       UUID,\n" +
          "       PATH,\n" +
          "       JOB_BATCH_ID,\n" +
          "       VIEW_MODE\n" +
          "FROM NOTES\n" +
          "WHERE ID = :ID;";

  private static final String GET_NOTE_BY_UUID = "" +
          "SELECT ID,\n" +
          "       UUID,\n" +
          "       PATH,\n" +
          "       JOB_BATCH_ID,\n" +
          "       VIEW_MODE\n" +
          "FROM NOTES\n" +
          "WHERE UUID = :UUID;";

  private static final String GET_ALL_NOTES = "" +
          "SELECT ID,\n" +
          "       UUID,\n" +
          "       PATH,\n" +
          "       JOB_BATCH_ID,\n" +
          "       VIEW_MODE\n" +
      "FROM NOTES\n";

  private static final String DELETE_NOTE = "" +
          "DELETE\n" +
          "FROM NOTES\n" +
          "WHERE ID = :ID;";

  private static final String GET_NOTE_PERMISSIONS = "" +
          "SELECT NAME,\n" +
          "       TYPE\n" +
          "FROM NOTE_PERMISSION\n" +
          "WHERE NOTE_ID = :NOTE_ID";

  private static final String PERSIST_PERMISSION = "" +
      "INSERT INTO NOTE_PERMISSION (NAME,\n" +
      "                             TYPE,\n" +
      "                             NOTE_ID)\n" +
      "VALUES (:NAME,\n" +
      "        :TYPE,\n" +
      "        :NOTE_ID);";

  private static final String DELETE_PERMISSION = "" +
      "DELETE\n" +
      "FROM NOTE_PERMISSION\n" +
      "WHERE NAME = :NAME AND TYPE = :TYPE AND NOTE_ID = :NOTE_ID;";

  private final NamedParameterJdbcTemplate jdbcTemplate;

  public NoteDAO(final NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  private static Map.Entry<String, String> mapPermission(final ResultSet resultSet,
                                                         final int i) throws SQLException {
    final String type = resultSet.getString("TYPE");
    final String name = resultSet.getString("NAME");
    return new AbstractMap.SimpleEntry<>(type, name);
  }

  private List<Map.Entry<String, String>> getAllPermissions(final long id) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("NOTE_ID", id);
    return jdbcTemplate.query(
        GET_NOTE_PERMISSIONS,
        parameters,
        NoteDAO::mapPermission);
  }

  private void setPermissions(final Note note) {
    getAllPermissions(note.getId()).forEach(pair -> {
      final String permissionType = pair.getKey();
      final String username = pair.getValue();
      if (permissionType.equals("OWNER")) {
        note.getOwners().add(username);
      } else if (permissionType.equals("READER")) {
        note.getReaders().add(username);
      } else if (permissionType.equals("WRITER")) {
        note.getWriters().add(username);
      } else if (permissionType.equals("RUNNER")) {
        note.getRunners().add(username);
      } else {
        throw new IllegalArgumentException("Wrong permission type: " + permissionType);
      }
     });
  }

  private void processPermissions(final Note note) {
    final List<Map.Entry<String, String>> allPermissions = new ArrayList<>();
    note.getOwners().forEach(v -> allPermissions.add(new SimpleEntry<>("OWNER", v)));
    note.getRunners().forEach(v -> allPermissions.add(new SimpleEntry<>("RUNNER", v)));
    note.getWriters().forEach(v -> allPermissions.add(new SimpleEntry<>("WRITER", v)));
    note.getReaders().forEach(v -> allPermissions.add(new SimpleEntry<>("READER", v)));
    final List<Map.Entry<String, String>> currentPermissions = getAllPermissions(note.getId());

    final List<Map.Entry<String, String>> newPermissions = new ArrayList<>(allPermissions);
    newPermissions.removeAll(currentPermissions);
    newPermissions.forEach(p -> persistPermission(note.getId(), p.getKey(), p.getValue()));

    final List<Map.Entry<String, String>> deletedPermissions = new ArrayList<>(currentPermissions);
    deletedPermissions.removeAll(allPermissions);
    deletedPermissions.forEach(p -> deletePermission(note.getId(), p.getKey(), p.getValue()));
  }

  private void persistPermission(final long noteId, final String type, final String name) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("NAME", name)
        .addValue("TYPE", type)
        .addValue("NOTE_ID", noteId);
    jdbcTemplate.update(PERSIST_PERMISSION, parameters);
  }

  private void deletePermission(final long noteId, final String type, final String name) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("NAME", name)
        .addValue("TYPE", type)
        .addValue("NOTE_ID", noteId);
    jdbcTemplate.update(DELETE_PERMISSION, parameters);
  }

  private static Note mapRow(final ResultSet resultSet, final int i) throws SQLException {
    final Type formParamsType = new TypeToken<Map<String, String>>() {}.getType();
    final long dbNoteId = resultSet.getLong("id");
    final String noteId = resultSet.getString("UUID");
    final String notePath = resultSet.getString("path");
    final Long jobBatchId = resultSet.getString("JOB_BATCH_ID") != null
            ? resultSet.getLong("JOB_BATCH_ID")
            : null;
    final NoteViewMode viewMode = resultSet.getString("VIEW_MODE") != null
        ? NoteViewMode.valueOf(resultSet.getString("VIEW_MODE"))
        : NoteViewMode.DEFAULT;

    final Note note = new Note(notePath);
    note.setId(dbNoteId);
    note.setUuid(noteId);
    note.setBatchJobId(jobBatchId);
    note.setViewMode(viewMode);
    return note;
  }

  public Note persist(final Note note) {
    //todo add permissions and form params

    final KeyHolder holder = new GeneratedKeyHolder();
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("UUID", note.getUuid())
            .addValue("PATH", note.getPath())
            .addValue("JOB_BATCH_ID", note.getBatchJobId())
            .addValue("VIEW_MODE", note.getViewMode().name());
    jdbcTemplate.update(PERSIST_NOTE, parameters, holder);
    note.setId((Long) holder.getKeys().get("id"));
    processPermissions(note);
    return note;
  }

  public Note update(final Note note) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("UUID", note.getUuid())
            .addValue("PATH", note.getPath())
            .addValue("JOB_BATCH_ID", note.getBatchJobId())
            .addValue("ID", note.getId())
            .addValue("VIEW_MODE", note.getViewMode().name());
    jdbcTemplate.update(UPDATE_NOTE, parameters);
    processPermissions(note);
    return note;
  }

  public Note get(final Long noteId) {
    //todo permissions and form params
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", noteId);
    final Optional<Note> result = jdbcTemplate.query(
        GET_NOTE_BY_ID,
        parameters,
        NoteDAO::mapRow)
        .stream()
        .findFirst();
    result.ifPresent(this::setPermissions);
    return result.orElse(null);
  }

  public Note get(final String uuid) {
    //todo permissions and form params
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("UUID", uuid);

    final Optional<Note> result = jdbcTemplate.query(
        GET_NOTE_BY_UUID,
        parameters,
        NoteDAO::mapRow)
        .stream()
        .findFirst();
    result.ifPresent(this::setPermissions);
    return result.orElse(null);
  }

  public void remove(final Note note) {
    jdbcTemplate.update(DELETE_NOTE, new MapSqlParameterSource("ID", note.getId()));
  }

  public List<Note> getAllNotes() {
    //todo permissions and form params
    final SqlParameterSource parameters = new MapSqlParameterSource();
    return jdbcTemplate.query(
        GET_ALL_NOTES,
        parameters,
        NoteDAO::mapRow)
        .stream()
        .peek(this::setPermissions)
        .collect(Collectors.toList());
  }
}

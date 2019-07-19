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

import static ru.tinkoff.zeppelin.storage.Utils.generatePGjson;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
          "                   PERMISSIONS,\n" +
          "                   FORM_PARAMS,\n" +
          "                   JOB_BATCH_ID,\n" +
          "                   VIEW_MODE)\n" +
          "VALUES (:UUID,\n" +
          "        :PATH,\n" +
          "        :PERMISSIONS,\n" +
          "        :FORM_PARAMS,\n" +
          "        :JOB_BATCH_ID,\n" +
          "        :VIEW_MODE);";

  private static final String UPDATE_NOTE = "" +
          "UPDATE NOTES\n" +
          "SET UUID         = :UUID,\n" +
          "    PATH         = :PATH,\n" +
          "    PERMISSIONS  = :PERMISSIONS,\n" +
          "    FORM_PARAMS  = :FORM_PARAMS,\n" +
          "    JOB_BATCH_ID = :JOB_BATCH_ID,\n" +
          "    VIEW_MODE    = :VIEW_MODE\n" +
          "WHERE ID = :ID;";


  private static final String GET_NOTE_BY_ID = "" +
          "SELECT ID,\n" +
          "       UUID,\n" +
          "       PATH,\n" +
          "       PERMISSIONS,\n" +
          "       FORM_PARAMS,\n" +
          "       JOB_BATCH_ID,\n" +
          "       VIEW_MODE\n" +
          "FROM NOTES\n" +
          "WHERE ID = :ID;";

  private static final String GET_NOTE_BY_UUID = "" +
          "SELECT ID,\n" +
          "       UUID,\n" +
          "       PATH,\n" +
          "       PERMISSIONS,\n" +
          "       FORM_PARAMS,\n" +
          "       JOB_BATCH_ID,\n" +
          "       VIEW_MODE\n" +
          "FROM NOTES\n" +
          "WHERE UUID = :UUID;";

  private static final String GET_ALL_NOTES = "" +
          "SELECT ID,\n" +
          "       UUID,\n" +
          "       PATH,\n" +
          "       PERMISSIONS,\n" +
          "       FORM_PARAMS,\n" +
          "       JOB_BATCH_ID,\n" +
          "       VIEW_MODE\n" +
      "FROM NOTES\n";

  private static final String DELETE_NOTE = "" +
          "DELETE\n" +
          "FROM NOTES\n" +
          "WHERE ID = :ID;";

  private final NamedParameterJdbcTemplate jdbcTemplate;

  private static final Gson gson = new Gson();

  public NoteDAO(final NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }


  private static Note mapRow(final ResultSet resultSet, final int i) throws SQLException {
    final Type formParamsType = new TypeToken<Map<String, String>>() {}.getType();
    final long dbNoteId = resultSet.getLong("id");
    final String noteId = resultSet.getString("UUID");
    final String notePath = resultSet.getString("path");
    final Map<String, String> formParams = gson.fromJson(resultSet.getString("FORM_PARAMS"), formParamsType);
    Map<String, Set<String>> permission = new HashMap<>(4);
    permission = gson.fromJson(resultSet.getString("permissions"), permission.getClass());
    final Long jobBatchId = resultSet.getString("JOB_BATCH_ID") != null
            ? resultSet.getLong("JOB_BATCH_ID")
            : null;
    final NoteViewMode viewMode = resultSet.getString("VIEW_MODE") != null
        ? NoteViewMode.valueOf(resultSet.getString("VIEW_MODE"))
        : NoteViewMode.DEFAULT;

    final Note note = new Note(notePath);
    note.setId(dbNoteId);
    note.setUuid(noteId);
    note.getOwners().addAll(permission.get("owners"));
    note.getReaders().addAll(permission.get("readers"));
    note.getRunners().addAll(permission.get("runners"));
    note.getWriters().addAll(permission.get("writers"));
    note.getFormParams().putAll(formParams);
    note.setBatchJobId(jobBatchId);
    note.setViewMode(viewMode);
    return note;
  }

  public Note persist(final Note note) {
    final HashMap<String, Set<String>> permission = new HashMap<>();
    permission.put("owners", note.getOwners());
    permission.put("readers", note.getReaders());
    permission.put("runners", note.getRunners());
    permission.put("writers", note.getWriters());

    final KeyHolder holder = new GeneratedKeyHolder();
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("UUID", note.getUuid())
            .addValue("PATH", note.getPath())
            .addValue("PERMISSIONS", generatePGjson(permission))
            .addValue("FORM_PARAMS", generatePGjson(note.getFormParams()))
            .addValue("JOB_BATCH_ID", note.getBatchJobId())
            .addValue("VIEW_MODE", note.getViewMode().name());
    jdbcTemplate.update(PERSIST_NOTE, parameters, holder);

    note.setId((Long) holder.getKeys().get("id"));
    return note;
  }

  public Note update(final Note note) {
    final HashMap<String, Set<String>> permission = new HashMap<>();
    permission.put("owners", note.getOwners());
    permission.put("readers", note.getReaders());
    permission.put("runners", note.getRunners());
    permission.put("writers", note.getWriters());

    final KeyHolder holder = new GeneratedKeyHolder();
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("UUID", note.getUuid())
            .addValue("PATH", note.getPath())
            .addValue("PERMISSIONS", generatePGjson(permission))
            .addValue("FORM_PARAMS", generatePGjson(note.getFormParams()))
            .addValue("JOB_BATCH_ID", note.getBatchJobId())
            .addValue("ID", note.getId())
            .addValue("VIEW_MODE", note.getViewMode().name());

    jdbcTemplate.update(UPDATE_NOTE, parameters, holder);
    return note;
  }

  public Note get(final Long noteId) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", noteId);

    return jdbcTemplate.query(
            GET_NOTE_BY_ID,
            parameters,
            NoteDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public Note get(final String uuid) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("UUID", uuid);

    return jdbcTemplate.query(
            GET_NOTE_BY_UUID,
            parameters,
            NoteDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public void remove(final Note note) {
    jdbcTemplate.update(DELETE_NOTE, new MapSqlParameterSource("ID", note.getId()));
  }

  public List<Note> getAllNotes() {
    final SqlParameterSource parameters = new MapSqlParameterSource();
    return jdbcTemplate.query(
            GET_ALL_NOTES,
            parameters,
            NoteDAO::mapRow);
  }
}

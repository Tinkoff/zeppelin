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
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;

@Component
public class RecentNotesDAO {

  static final int RECENT_NOTES_PER_USER_LIMIT = 5;

  private static final String GET_USER_RECENT_NOTES = "" +
      "SELECT NOTE_UUID FROM RECENT_NOTES\n" +
      "WHERE USER_NAME = :USER_NAME\n" +
      "ORDER BY VISIT_TIME;";

  private static final String VISIT =  "" +
      "INSERT INTO RECENT_NOTES (NOTE_UUID,\n" +
      "                          USER_NAME)\n" +
      "VALUES (:NOTE_UUID,\n" +
      "        :USER_NAME)\n" +
      "ON CONFLICT (NOTE_UUID, USER_NAME) DO UPDATE\n" +
      "SET VISIT_TIME = NOW();";

  private static final String DELETE = "" +
      "WITH COUNTED AS (\n" +
      "   SELECT ID, ROW_NUMBER() OVER (\n" +
      "       PARTITION BY USER_NAME ORDER BY VISIT_TIME DESC\n" +
      "   ) AS ROW_NUMBER\n" +
      "   FROM RECENT_NOTES\n" +
      ")\n" +
      "DELETE FROM RECENT_NOTES WHERE ID IN (\n" +
      "   SELECT ID FROM COUNTED\n"  +
      "   WHERE ROW_NUMBER > :LIMIT\n" +
      ");";

  private final NamedParameterJdbcTemplate jdbcTemplate;


  public RecentNotesDAO(final NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  private static String mapRow(final ResultSet resultSet, final int i) throws SQLException {
    return resultSet.getString("NOTE_UUID");
  }

  public void persist(@Nonnull final String uuid,
                      @Nonnull final String username) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("NOTE_UUID", uuid)
        .addValue("USER_NAME", username);
    jdbcTemplate.update(VISIT, parameters);
  }

  public void cleanup() {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("LIMIT", RECENT_NOTES_PER_USER_LIMIT);
    jdbcTemplate.update(DELETE, parameters);
  }

  public List<String> getAll(final String username) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("USER_NAME", username);

    return jdbcTemplate.query(
        GET_USER_RECENT_NOTES,
        parameters,
        RecentNotesDAO::mapRow);
  }

}

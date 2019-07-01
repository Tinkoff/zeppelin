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

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Component
public class ModuleSourcesDAO {

  private static final String GET_ALL = "" +
          "SELECT ID,\n" +
          "       NAME,\n" +
          "       TYPE,\n" +
          "       ARTIFACT,\n" +
          "       STATUS,\n" +
          "       PATH,\n" +
          "       REINSTALL_ON_START\n" +
          "FROM MODULE_SOURCE;";


  private static final String GET_BY_ID = "" +
          "SELECT ID,\n" +
          "       NAME,\n" +
          "       TYPE,\n" +
          "       ARTIFACT,\n" +
          "       STATUS,\n" +
          "       PATH,\n" +
          "       REINSTALL_ON_START\n" +
          "FROM MODULE_SOURCE\n" +
          "WHERE ID = :ID;";

  private static final String PERSIST = "" +
          "INSERT INTO MODULE_SOURCE (NAME,\n" +
          "                           TYPE,\n" +
          "                           ARTIFACT,\n" +
          "                           STATUS,\n" +
          "                           PATH,\n" +
          "                           REINSTALL_ON_START)\n" +
          "VALUES (:NAME,\n" +
          "        :TYPE,\n" +
          "        :ARTIFACT,\n" +
          "        :STATUS,\n" +
          "        :PATH,\n" +
          "        :REINSTALL_ON_START);";

  private static final String UPDATE = "" +
          "UPDATE MODULE_SOURCE\n" +
          "SET NAME = :NAME,\n" +
          "    TYPE = :TYPE,\n" +
          "    ARTIFACT = :ARTIFACT,\n" +
          "    STATUS = :STATUS,\n" +
          "    PATH = :PATH,\n" +
          "    REINSTALL_ON_START = :REINSTALL_ON_START\n" +
          "WHERE ID = :ID;";

  private static final String DELETE = "" +
          "DELETE FROM MODULE_SOURCE WHERE ID = :ID;";

  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  public ModuleSourcesDAO(final NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }

  private static ModuleSource mapRow(final ResultSet resultSet, final int i) throws SQLException {
    return new ModuleSource(
            resultSet.getLong("ID"),
            resultSet.getString("NAME"),
            ModuleSource.Type.valueOf(resultSet.getString("TYPE")),
            resultSet.getString("ARTIFACT"),
            ModuleSource.Status.valueOf(resultSet.getString("STATUS")),
            resultSet.getString("PATH"),
            resultSet.getBoolean("REINSTALL_ON_START")
    );
  }

  public List<ModuleSource> getAll() {

    final SqlParameterSource parameters = new MapSqlParameterSource();

    return namedParameterJdbcTemplate.query(
            GET_ALL,
            parameters,
            ModuleSourcesDAO::mapRow);
  }


  public ModuleSource get(final long id) {

    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", id);

    return namedParameterJdbcTemplate.query(
            GET_BY_ID,
            parameters,
            ModuleSourcesDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }


  public void update(final ModuleSource source) {

    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", source.getId())
            .addValue("NAME", source.getName())
            .addValue("TYPE", source.getType().name())
            .addValue("ARTIFACT", source.getArtifact())
            .addValue("STATUS", source.getStatus().name())
            .addValue("PATH", source.getPath())
            .addValue("REINSTALL_ON_START", source.isReinstallOnStart());

    namedParameterJdbcTemplate.update(UPDATE, parameters);
  }

  public ModuleSource persist(final ModuleSource source) {
    final KeyHolder holder = new GeneratedKeyHolder();
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("NAME", source.getName())
            .addValue("TYPE", source.getType().name())
            .addValue("ARTIFACT", source.getArtifact())
            .addValue("STATUS", source.getStatus().name())
            .addValue("PATH", source.getPath())
            .addValue("REINSTALL_ON_START", source.isReinstallOnStart());

    namedParameterJdbcTemplate.update(PERSIST, parameters, holder);
    source.setId((Long) holder.getKeys().get("id"));
    return source;
  }

  public void delete(final long id) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", id);

    namedParameterJdbcTemplate.update(DELETE, parameters);
  }
}

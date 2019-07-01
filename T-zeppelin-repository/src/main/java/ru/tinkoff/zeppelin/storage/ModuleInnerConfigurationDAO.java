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

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleProperty;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;


@Component
public class ModuleInnerConfigurationDAO {

  public static final String GET_ALL = "" +
          "SELECT ID,\n" +
          "       CLASS_NAME,\n" +
          "       PROPERTIES,\n" +
          "       EDITOR\n" +
          "FROM MODULE_INNER_CONFIGURATION;";

  public static final String GET_BY_ID = "" +
          "SELECT ID,\n" +
          "       CLASS_NAME,\n" +
          "       PROPERTIES,\n" +
          "       EDITOR\n" +
          "FROM MODULE_INNER_CONFIGURATION\n" +
          "WHERE ID = :ID;";


  private static final String PERSIST = "" +
          "INSERT INTO MODULE_INNER_CONFIGURATION (CLASS_NAME,\n" +
          "                                        PROPERTIES,\n" +
          "                                        EDITOR)\n" +
          "VALUES (:CLASS_NAME,\n" +
          "        :PROPERTIES,\n" +
          "        :EDITOR);";

  private static final String UPDATE = "" +
          "UPDATE MODULE_INNER_CONFIGURATION\n" +
          "SET CLASS_NAME = :CLASS_NAME,\n" +
          "    PROPERTIES = :PROPERTIES,\n" +
          "    EDITOR     = :EDITOR\n" +
          "WHERE ID = :ID;";

  private static final String DELETE = "" +
          "DELETE\n" +
          "FROM MODULE_INNER_CONFIGURATION\n" +
          "WHERE ID = :ID;";

  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;


  public ModuleInnerConfigurationDAO(final NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }

  private static ModuleInnerConfiguration mapRow(final ResultSet resultSet, final int i) throws SQLException {
    Preconditions.checkNotNull(resultSet);

    final long id = resultSet.getLong("id");
    final String className = resultSet.getString("class_name");

    final Map<String, ModuleProperty> properties =
            new Gson().fromJson(
                    resultSet.getString("properties"),
                    new TypeToken<Map<String, ModuleProperty>>() {
                    }.getType()
            );
    final Map<String, Object> editor =
            new Gson().fromJson(
                    resultSet.getString("editor"),
                    new TypeToken<Map<String, Object>>() {
                    }.getType()
            );
    return new ModuleInnerConfiguration(id, className, properties, editor);
  }

  public List<ModuleInnerConfiguration> getAll() {

    final SqlParameterSource parameters = new MapSqlParameterSource();

    return namedParameterJdbcTemplate.query(
            GET_ALL,
            parameters,
            ModuleInnerConfigurationDAO::mapRow
    );
  }

  public ModuleInnerConfiguration getById(final long id) {

    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", id);

    return namedParameterJdbcTemplate.query(
            GET_BY_ID,
            parameters,
            ModuleInnerConfigurationDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public ModuleInnerConfiguration persist(final ModuleInnerConfiguration config) {
    final KeyHolder holder = new GeneratedKeyHolder();

    final MapSqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("CLASS_NAME", config.getClassName())
            .addValue("PROPERTIES", Utils.generatePGjson(config.getProperties()))
            .addValue("EDITOR", Utils.generatePGjson(config.getEditor()));
    namedParameterJdbcTemplate.update(PERSIST, parameters, holder);

    config.setId((Long) holder.getKeys().get("id"));
    return config;
  }


  public ModuleInnerConfiguration update(final ModuleInnerConfiguration config) {

    final MapSqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", config.getId())
            .addValue("CLASS_NAME", config.getClassName())
            .addValue("PROPERTIES", Utils.generatePGjson(config.getProperties()))
            .addValue("EDITOR", Utils.generatePGjson(config.getEditor()));
    namedParameterJdbcTemplate.update(UPDATE, parameters);

    return config;
  }

  public void delete(final long id) {

    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", id);

    namedParameterJdbcTemplate.update(DELETE, parameters);
  }
}

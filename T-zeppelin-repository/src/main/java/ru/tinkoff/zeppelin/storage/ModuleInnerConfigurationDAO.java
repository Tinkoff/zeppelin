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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleProperty;


@Component
public class ModuleInnerConfigurationDAO {

  private static final String GET_ALL = "" +
          "SELECT ID,\n" +
          "       CLASS_NAME\n" +
          "FROM MODULE_INNER_CONFIGURATION;";

  private static final String GET_BY_ID = "" +
          "SELECT ID,\n" +
          "       CLASS_NAME\n" +
          "FROM MODULE_INNER_CONFIGURATION\n" +
          "WHERE ID = :ID;";


  private static final String PERSIST = "" +
          "INSERT INTO MODULE_INNER_CONFIGURATION (CLASS_NAME)\n" +
          "VALUES (:CLASS_NAME);";

  private static final String UPDATE = "" +
          "UPDATE MODULE_INNER_CONFIGURATION\n" +
          "SET CLASS_NAME = :CLASS_NAME\n" +
          "WHERE ID = :ID;";

  private static final String DELETE = "" +
          "DELETE\n" +
          "FROM MODULE_INNER_CONFIGURATION\n" +
          "WHERE ID = :ID;";

  private static final String PERSIST_PROPERTY = "" +
      "INSERT INTO MODULE_INNER_CONFIGURATION_PROPERTY(PROPERTY_NAME,\n" +
      "                                                MODULE_INNER_CONFIGURATION_ID)\n" +
      "VALUES (:PROPERTY_NAME,\n" +
      "        :MODULE_INNER_CONFIGURATION_ID);";

  private static final String PERSIST_PROPERTY_DETAILS = "" +
      "INSERT INTO MODULE_INNER_CONFIGURATION_PROPERTY_DETAIL(ENV_NAME,\n" +
      "                                                       TYPE,\n" +
      "                                                       KEY,\n" +
      "                                                       VALUE,\n" +
      "                                                       DEFAULT_VALUE,\n" +
      "                                                       DESCRIPTION,\n" +
      "                                                       MODULE_INNER_CONFIGURATION_PROPERTY_ID)\n" +
      "VALUES (:ENV_NAME,\n" +
      "        :TYPE,\n" +
      "        :KEY,\n" +
      "        :VALUE,\n" +
      "        :DEFAULT_VALUE,\n" +
      "        :DESCRIPTION,\n" +
      "        :MODULE_INNER_CONFIGURATION_PROPERTY_ID);";

  private static final String GET_PROPERTIES = "" +
      "SELECT P.ID,\n" +
      "       P.PROPERTY_NAME,\n" +
      "       D.ENV_NAME,\n" +
      "       D.TYPE,\n" +
      "       D.KEY,\n" +
      "       D.VALUE,\n" +
      "       D.DEFAULT_VALUE,\n" +
      "       D.DESCRIPTION\n" +
      "FROM MODULE_INNER_CONFIGURATION_PROPERTY P\n" +
      "JOIN MODULE_INNER_CONFIGURATION_PROPERTY_DETAIL D\n"+
      "    ON P.ID = D.MODULE_INNER_CONFIGURATION_PROPERTY_ID\n" +
      "WHERE MODULE_INNER_CONFIGURATION_ID = :MODULE_INNER_CONFIGURATION_ID;";

  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;


  public ModuleInnerConfigurationDAO(final NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }

  private static ModuleInnerConfiguration mapRow(final ResultSet resultSet, final int i) throws SQLException {
    Preconditions.checkNotNull(resultSet);

    final long id = resultSet.getLong("id");
    final String className = resultSet.getString("class_name");

    final Map<String, ModuleProperty> properties = new HashMap<>();
    final Map<String, Object> editor = new HashMap<>();
    return new ModuleInnerConfiguration(id, className, properties, editor);
  }

  public List<ModuleInnerConfiguration> getAll() {
    final SqlParameterSource parameters = new MapSqlParameterSource();
    return namedParameterJdbcTemplate.query(
        GET_ALL,
        parameters,
        ModuleInnerConfigurationDAO::mapRow)
        .stream()
        .peek(c -> {
          c.getProperties().clear();
          c.getProperties().putAll(getProperties(c.getId()));
        })
        .collect(Collectors.toList());
  }

  public ModuleInnerConfiguration getById(final long id) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", id);
    final Optional<ModuleInnerConfiguration> result =
        namedParameterJdbcTemplate.query(
            GET_BY_ID,
            parameters,
            ModuleInnerConfigurationDAO::mapRow)
            .stream()
            .findFirst();

    result.ifPresent(c -> {
      c.getProperties().clear();
      c.getProperties().putAll(getProperties(c.getId()));
    });
    return result.orElse(null);
  }

  public ModuleInnerConfiguration persist(final ModuleInnerConfiguration config) {
    final KeyHolder holder = new GeneratedKeyHolder();

    final MapSqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("CLASS_NAME", config.getClassName());
    namedParameterJdbcTemplate.update(PERSIST, parameters, holder);

    config.setId((Long) holder.getKeys().get("id"));
    persistProperties(config);
    return config;
  }

  private void persistProperties(final ModuleInnerConfiguration configuration) {
    final long configId = configuration.getId();
    final KeyHolder holder = new GeneratedKeyHolder();

    configuration.getProperties().forEach((key, value) -> {
      final MapSqlParameterSource propertyParameters = new MapSqlParameterSource()
          .addValue("PROPERTY_NAME", key)
          .addValue("MODULE_INNER_CONFIGURATION_ID", configId);
      namedParameterJdbcTemplate.update(PERSIST_PROPERTY, propertyParameters, holder);
      final long propertyId = (Long) holder.getKeys().get("id");

      final MapSqlParameterSource propertyDetails = new MapSqlParameterSource()
          .addValue("ENV_NAME", value.getEnvName())
          .addValue("TYPE",value.getType().toUpperCase())
          .addValue("KEY", value.getPropertyName())
          .addValue("VALUE", value.getCurrentValue())
          .addValue("DEFAULT_VALUE", value.getDefaultValue())
          .addValue("DESCRIPTION", value.getDescription())
          .addValue("MODULE_INNER_CONFIGURATION_PROPERTY_ID", propertyId);
      namedParameterJdbcTemplate.update(PERSIST_PROPERTY_DETAILS, propertyDetails);
    });
  }

  private static class ModuleInnerConfigurationPropertyDTO {
    public long id;
    public String propertyName;
    public String envName;
    public String type;
    public String key;
    public String value;
    public String defaultValue;
    public String description;
  }

  private Map<String, ModuleProperty> getProperties(final long configurationId) {
    final RowMapper<ModuleInnerConfigurationPropertyDTO> mapper = ((rs, rowNumber) -> {
      final ModuleInnerConfigurationPropertyDTO result = new ModuleInnerConfigurationPropertyDTO();
      result.id = rs.getLong("ID");
      result.propertyName = rs.getString("PROPERTY_NAME");
      result.envName = rs.getString("ENV_NAME");
      result.type = rs.getString("TYPE");
      result.key = rs.getString("KEY");
      result.value = rs.getString("VALUE");
      result.defaultValue = rs.getString("DEFAULT_VALUE");
      result.description = rs.getString("DESCRIPTION");
      return  result;
    });

    return namedParameterJdbcTemplate.query(
        GET_PROPERTIES,
        new MapSqlParameterSource().addValue("MODULE_INNER_CONFIGURATION_ID", configurationId),
        mapper)
        .stream()
        .collect(
            Collectors.toMap(
                p -> p.propertyName,
                p -> new ModuleProperty(p.envName, p.key, p.defaultValue, p.value, p.description, p.type)
            )
        );
  }


  public ModuleInnerConfiguration update(final ModuleInnerConfiguration config) {

    final MapSqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", config.getId())
            .addValue("CLASS_NAME", config.getClassName());
    namedParameterJdbcTemplate.update(UPDATE, parameters);

    return config;
  }

  public void delete(final long id) {

    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", id);

    namedParameterJdbcTemplate.update(DELETE, parameters);
  }
}

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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.option.Permissions;

@Component
public class ModuleConfigurationDAO {

  public static final String GET_ALL = "" +
      "SELECT ID,\n" +
      "       SHEBANG,\n" +
      "       HUMAN_READABLE_NAME,\n" +
      "       BINDED_TO,\n" +
      "       JVM_OPTIONS,\n" +
      "       CONCURRENT_TASKS,\n" +
      "       REGULAR_TTL,\n" +
      "       SCHEDULED_TTL,\n" +
      "       CONCURRENT_TASKS,\n" +
      "       CONFIG_ID,\n" +
      "       SOURCE_ID,\n" +
      "       IS_ENABLED\n" +
      "FROM MODULE_CONFIGURATION;";

  public static final String GET_BY_ID = "" +
      "SELECT ID,\n" +
      "       SHEBANG,\n" +
      "       HUMAN_READABLE_NAME,\n" +
      "       BINDED_TO,\n" +
      "       JVM_OPTIONS,\n" +
      "       CONCURRENT_TASKS,\n" +
      "       REGULAR_TTL,\n" +
      "       SCHEDULED_TTL,\n" +
      "       CONFIG_ID,\n" +
      "       SOURCE_ID,\n" +
      "       IS_ENABLED\n" +
      "FROM MODULE_CONFIGURATION\n" +
      "WHERE ID = :ID;";

  public static final String GET_BY_SHEBANG = "" +
      "SELECT ID,\n" +
      "       SHEBANG,\n" +
      "       HUMAN_READABLE_NAME,\n" +
      "       BINDED_TO,\n" +
      "       JVM_OPTIONS,\n" +
      "       CONCURRENT_TASKS,\n" +
      "       REGULAR_TTL,\n" +
      "       SCHEDULED_TTL,\n" +
      "       CONFIG_ID,\n" +
      "       SOURCE_ID,\n" +
      "       IS_ENABLED\n" +
      "FROM MODULE_CONFIGURATION\n" +
      "WHERE SHEBANG = :SHEBANG;";

  private static final String PERSIST_MODULE_PERMISSION = "" +
      "INSERT INTO MODULE_CONFIGURATION_PERMISSION(IS_ENABLED,\n" +
      "                                            MODULE_CONFIGURATION_ID)\n" +
      "VALUES (:IS_ENABLED,\n" +
      "        :MODULE_CONFIGURATION_ID);";

  private static final String UPDATE_MODULE_PERMISSION = "" +
      "UPDATE MODULE_CONFIGURATION_PERMISSION\n" +
      "SET IS_ENABLED = :IS_ENABLED\n" +
      "WHERE MODULE_CONFIGURATION_ID = :MODULE_CONFIGURATION_ID;";

  private static final String PERSIST_MODULE_OWNER = "" +
      "INSERT INTO MODULE_CONFIGURATION_OWNER(NAME,\n" +
      "                                       MODULE_CONFIGURATION_PERMISSION_ID)\n" +
      "VALUES (:NAME,\n" +
      "        :MODULE_CONFIGURATION_PERMISSION_ID);";

  private static final String DELETE_MODULE_OWNER = "" +
      "DELETE FROM MODULE_CONFIGURATION_OWNER\n" +
      "WHERE NAME = :NAME\n" +
      "      AND MODULE_CONFIGURATION_PERMISSION_ID = :MODULE_CONFIGURATION_PERMISSION_ID;";

  private static final String GET_MODULE_PERMISSION = "" +
      "SELECT ID,\n" +
      "       IS_ENABLED\n" +
      "FROM MODULE_CONFIGURATION_PERMISSION\n" +
      "WHERE MODULE_CONFIGURATION_ID = :MODULE_CONFIGURATION_ID;";

  private static final String GET_MODULE_OWNERS = "" +
      "SELECT NAME\n" +
      "FROM MODULE_CONFIGURATION_OWNER\n" +
      "WHERE MODULE_CONFIGURATION_PERMISSION_ID = :MODULE_CONFIGURATION_PERMISSION_ID;";

  private static final String PERSIST = "" +
      "INSERT INTO MODULE_CONFIGURATION (SHEBANG,\n" +
      "                                  HUMAN_READABLE_NAME,\n" +
      "                                  BINDED_TO,\n" +
      "                                  JVM_OPTIONS,\n" +
      "                                  CONCURRENT_TASKS,\n" +
      "                                  REGULAR_TTL,\n" +
      "                                  SCHEDULED_TTL,\n" +
      "                                  CONFIG_ID,\n" +
      "                                  SOURCE_ID,\n" +
      "                                  IS_ENABLED)\n" +
      "VALUES (:SHEBANG,\n" +
      "        :HUMAN_READABLE_NAME,\n" +
      "        :BINDED_TO,\n" +
      "        :JVM_OPTIONS,\n" +
      "        :CONCURRENT_TASKS,\n" +
      "        :REGULAR_TTL,\n" +
      "        :SCHEDULED_TTL,\n" +
      "        :CONFIG_ID,\n" +
      "        :SOURCE_ID,\n" +
      "        :IS_ENABLED);";

  private static final String UPDATE = "" +
      "UPDATE MODULE_CONFIGURATION\n" +
      "SET SHEBANG             = :SHEBANG,\n" +
      "    HUMAN_READABLE_NAME = :HUMAN_READABLE_NAME,\n" +
      "    BINDED_TO           = :BINDED_TO,\n" +
      "    JVM_OPTIONS         = :JVM_OPTIONS,\n" +
      "    CONCURRENT_TASKS    = :CONCURRENT_TASKS,\n" +
      "    REGULAR_TTL         = :REGULAR_TTL,\n" +
      "    SCHEDULED_TTL       = :SCHEDULED_TTL,\n" +
      "    CONFIG_ID           = :CONFIG_ID,\n" +
      "    SOURCE_ID           = :SOURCE_ID,\n" +
      "    IS_ENABLED          = :IS_ENABLED\n" +
      "WHERE ID = :ID;";

  private static final String DELETE = "" +
      "DELETE\n" +
      "FROM MODULE_CONFIGURATION\n" +
      "WHERE ID = :ID;";

  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;


  public ModuleConfigurationDAO(final NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }

  private static ModuleConfiguration mapRow(final ResultSet resultSet, final int i) throws SQLException {
    return new ModuleConfiguration(
        resultSet.getLong("ID"),
        resultSet.getString("SHEBANG"),
        resultSet.getString("HUMAN_READABLE_NAME"),
        resultSet.getString("BINDED_TO"),
        resultSet.getString("JVM_OPTIONS"),
        resultSet.getInt("CONCURRENT_TASKS"),
        resultSet.getInt("REGULAR_TTL"),
        resultSet.getInt("SCHEDULED_TTL"),
        resultSet.getLong("CONFIG_ID"),
        resultSet.getLong("SOURCE_ID"),
        new Permissions(),
        resultSet.getBoolean("IS_ENABLED")
    );
  }

  private void createPermissions(final ModuleConfiguration configuration) {
    final MapSqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("IS_ENABLED", configuration.getPermissions().isEnabled())
        .addValue("MODULE_CONFIGURATION_ID", configuration.getId());
    namedParameterJdbcTemplate.update(PERSIST_MODULE_PERMISSION, parameters);
  }

  private void persistOwner(final String name, final long permissionId) {
    final MapSqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("NAME", name)
        .addValue("MODULE_CONFIGURATION_PERMISSION_ID", permissionId);
    namedParameterJdbcTemplate.update(PERSIST_MODULE_OWNER, parameters);
  }

  private void deleteOwner(final String name, final long permissionId) {
    final MapSqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("NAME", name)
        .addValue("MODULE_CONFIGURATION_PERMISSION_ID", permissionId);
    namedParameterJdbcTemplate.update(DELETE_MODULE_OWNER, parameters);
  }

  private void updatePermissions(final boolean isEnabled, final long configurationId) {
    final MapSqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("IS_ENABLED", isEnabled)
        .addValue("MODULE_CONFIGURATION_ID", configurationId);
    namedParameterJdbcTemplate.update(UPDATE_MODULE_PERMISSION, parameters);
  }

  private void processPermissions(final ModuleConfiguration configuration) {
    updatePermissions(configuration.getPermissions().isEnabled(), configuration.getId());
    final long permissionId = getStoredPermission(configuration).databaseId;
    final Permissions storedPermissions = getPermissions(configuration);
    final List<String> newOwners = new ArrayList<>(configuration.getPermissions().getOwners());
    newOwners.removeAll(storedPermissions.getOwners());
    newOwners.forEach(o -> persistOwner(o, permissionId));

    final List<String> deletedOwners = new ArrayList<>(storedPermissions.getOwners());
    deletedOwners.removeAll(configuration.getPermissions().getOwners());
    deletedOwners.forEach(o -> deleteOwner(o, permissionId));
  }

  private static class PermissionsDTO {
    long databaseId;
    Boolean isEnabled;
  }

  private PermissionsDTO getStoredPermission(final ModuleConfiguration configuration) {
    final RowMapper<PermissionsDTO> rowMapper = (rs, rowNum) -> {
      final PermissionsDTO p = new PermissionsDTO();
      p.isEnabled = rs.getBoolean("IS_ENABLED");
      p.databaseId = rs.getLong("ID");
      return p;
    };

    final PermissionsDTO permissions =
        namedParameterJdbcTemplate.query(
            GET_MODULE_PERMISSION,
            new MapSqlParameterSource().addValue("MODULE_CONFIGURATION_ID", configuration.getId()),
            rowMapper)
            .stream()
            .findFirst()
            .orElse(null);
    if (permissions == null) {
      throw new IllegalStateException("Permission doesn't exit, moduleId = " + configuration.getId());
    }
    return permissions;
  }

  private Permissions getPermissions(final ModuleConfiguration configuration) {
    final PermissionsDTO permissions = getStoredPermission(configuration);
    final RowMapper<String> ownerMapper = ((rs, rowNum) -> rs.getString("NAME"));
    final List<String> owners =
        new ArrayList<>(
            namedParameterJdbcTemplate.query(
                GET_MODULE_OWNERS,
                new MapSqlParameterSource()
                    .addValue("MODULE_CONFIGURATION_PERMISSION_ID", permissions.databaseId),
                ownerMapper
            )
        );
    return new Permissions(owners, permissions.isEnabled);
  }

  public List<ModuleConfiguration> getAll() {

    final SqlParameterSource parameters = new MapSqlParameterSource();

    return namedParameterJdbcTemplate.query(
        GET_ALL,
        parameters,
        ModuleConfigurationDAO::mapRow)
        .stream()
        .peek(c -> c.setPermissions(getPermissions(c)))
        .collect(Collectors.toList());
  }

  public ModuleConfiguration getById(final long id) {

    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("ID", id);

    final Optional<ModuleConfiguration> result = namedParameterJdbcTemplate.query(
        GET_BY_ID,
        parameters,
        ModuleConfigurationDAO::mapRow)
        .stream()
        .findFirst();
    result.ifPresent(c -> c.setPermissions(getPermissions(c)));
    return result.orElse(null);
  }

  public ModuleConfiguration getByShebang(final String shebang) {

    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("SHEBANG", shebang);

    final Optional<ModuleConfiguration> result = namedParameterJdbcTemplate.query(
        GET_BY_SHEBANG,
        parameters,
        ModuleConfigurationDAO::mapRow)
        .stream()
        .findFirst();
    result.ifPresent(c -> c.setPermissions(getPermissions(c)));
    return result.orElse(null);
  }

  public ModuleConfiguration persist(final ModuleConfiguration config) {
    final KeyHolder holder = new GeneratedKeyHolder();

    final MapSqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("SHEBANG", config.getShebang())
        .addValue("HUMAN_READABLE_NAME", config.getHumanReadableName())
        .addValue("BINDED_TO", config.getBindedTo())
        .addValue("JVM_OPTIONS", config.getJvmOptions())
        .addValue("CONCURRENT_TASKS", config.getConcurrentTasks())
        .addValue("REGULAR_TTL", config.getRegularTTL())
        .addValue("SCHEDULED_TTL", config.getScheduledTTL())
        .addValue("CONFIG_ID", config.getModuleInnerConfigId())
        .addValue("SOURCE_ID", config.getModuleSourceId())
        .addValue("IS_ENABLED", config.isEnabled());
    namedParameterJdbcTemplate.update(PERSIST, parameters, holder);

    config.setId((Long) holder.getKeys().get("id"));
    createPermissions(config);
    processPermissions(config);
    return config;
  }


  public ModuleConfiguration update(final ModuleConfiguration config) {

    final MapSqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("SHEBANG", config.getShebang())
        .addValue("HUMAN_READABLE_NAME", config.getHumanReadableName())
        .addValue("BINDED_TO", config.getBindedTo())
        .addValue("JVM_OPTIONS", config.getJvmOptions())
        .addValue("CONCURRENT_TASKS", config.getConcurrentTasks())
        .addValue("REGULAR_TTL", config.getRegularTTL())
        .addValue("SCHEDULED_TTL", config.getScheduledTTL())
        .addValue("CONFIG_ID", config.getModuleInnerConfigId())
        .addValue("SOURCE_ID", config.getModuleSourceId())
        .addValue("IS_ENABLED", config.isEnabled())
        .addValue("ID", config.getId());
    namedParameterJdbcTemplate.update(UPDATE, parameters);
    processPermissions(config);
    return config;
  }

  public void delete(final long id) {

    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("ID", id);

    namedParameterJdbcTemplate.update(DELETE, parameters);
  }
}

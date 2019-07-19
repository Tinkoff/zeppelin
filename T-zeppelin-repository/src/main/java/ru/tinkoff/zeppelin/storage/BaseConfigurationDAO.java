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
  import org.springframework.stereotype.Component;
  import ru.tinkoff.zeppelin.core.configuration.system.BaseConfiguration;

  import java.sql.ResultSet;
  import java.sql.SQLException;
  import java.util.List;

  @Component
  public class BaseConfigurationDAO {

    private static final String UPDATE_BASE_CONFIGURATION = "" +
            "UPDATE BASE_CONFIGURATION\n" +
            "SET VALUE                 = :VALUE\n" +
            "WHERE NAME = :NAME;";

    private static final String SELECT_BASE_CONFIGURATION_BY_NAME = "" +
            "SELECT ID,\n" +
            "       NAME,\n" +
            "       VALUE,\n" +
            "       TYPE,\n" +
            "       DESCRIPTION\n" +
            "FROM BASE_CONFIGURATION\n" +
            "WHERE NAME = :NAME;";

    private static final String SELECT_BASE_CONFIGURATION = "" +
            "SELECT ID,\n" +
            "       NAME,\n" +
            "       VALUE,\n" +
            "       TYPE,\n" +
            "       DESCRIPTION\n" +
            "FROM BASE_CONFIGURATION;";


    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public BaseConfigurationDAO(final NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
      this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    }

    @SuppressWarnings("unchecked")
    private static BaseConfiguration mapRow(final ResultSet resultSet, final int i) throws SQLException {
      final Long id = resultSet.getLong("ID");
      final String name = resultSet.getString("NAME");
      final BaseConfiguration.Type type = BaseConfiguration.Type.valueOf(resultSet.getString("TYPE"));
      final String description = resultSet.getString("DESCRIPTION");
      final Object val;
      switch (type) {
        case BOOLEAN:
          val = resultSet.getBoolean("VALUE");
          break;
        case STRING:
          val = resultSet.getLong("VALUE");
          break;
        case PASSWORD:
          val = "*********";
          break;
        default:
          val = resultSet.getString("VALUE");
      }

      return new BaseConfiguration(id, name, val, type, description);
    }


    public void update(final String name, final String value) {
      final SqlParameterSource parameters = new MapSqlParameterSource()
              .addValue("NAME", name)
              .addValue("VALUE", value);
      namedParameterJdbcTemplate.update(UPDATE_BASE_CONFIGURATION, parameters);
    }

    public BaseConfiguration getByName(final String name) {
      final SqlParameterSource parameters = new MapSqlParameterSource()
              .addValue("NAME", name);

      return namedParameterJdbcTemplate.query(
              SELECT_BASE_CONFIGURATION_BY_NAME,
              parameters,
              BaseConfigurationDAO::mapRow)
              .stream()
              .findFirst()
              .orElse(null);
    }

    public List<BaseConfiguration> get() {
      return namedParameterJdbcTemplate.query(
              SELECT_BASE_CONFIGURATION,
              BaseConfigurationDAO::mapRow);
    }
  }

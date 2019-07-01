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
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.zeppelin.Repository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;

@Component
public class ModuleRepositoryDAO {

  private static final String GET_ALL_REPOSITORIES = "" +
      "SELECT ID,\n" +
      "       REPOSITORY_ID,\n" +
      "       SNAPSHOT,\n" +
      "       URL,\n" +
      "       USERNAME,\n" +
      "       PASSWORD,\n" +
      "       PROXY_PROTOCOL,\n" +
      "       PROXY_HOST,\n" +
      "       PROXY_PORT,\n" +
      "       PROXY_LOGIN,\n" +
      "       PROXY_PASSWORD\n" +
      "FROM MODULE_REPOSITORY;";

  private static final String GET_BY_ID = "" +
      "SELECT ID,\n" +
      "       REPOSITORY_ID,\n" +
      "       SNAPSHOT,\n" +
      "       URL,\n" +
      "       USERNAME,\n" +
      "       PASSWORD,\n" +
      "       PROXY_PROTOCOL,\n" +
      "       PROXY_HOST,\n" +
      "       PROXY_PORT,\n" +
      "       PROXY_LOGIN,\n" +
      "       PROXY_PASSWORD\n" +
      "FROM MODULE_REPOSITORY\n" +
      "WHERE REPOSITORY_ID = :REPOSITORY_ID;";

  private static final String PERSIST = "" +
      "INSERT INTO MODULE_REPOSITORY (REPOSITORY_ID,\n" +
      "                               SNAPSHOT,\n" +
      "                               URL,\n" +
      "                               USERNAME,\n" +
      "                               PASSWORD,\n" +
      "                               PROXY_PROTOCOL,\n" +
      "                               PROXY_HOST,\n" +
      "                               PROXY_PORT,\n" +
      "                               PROXY_LOGIN,\n" +
      "                               PROXY_PASSWORD)\n" +
      "VALUES (:REPOSITORY_ID,\n" +
      "        :SNAPSHOT,\n" +
      "        :URL,\n" +
      "        :USERNAME,\n" +
      "        :PASSWORD,\n" +
      "        :PROXY_PROTOCOL,\n" +
      "        :PROXY_HOST,\n" +
      "        :PROXY_PORT,\n" +
      "        :PROXY_LOGIN,\n" +
      "        :PROXY_PASSWORD);";


  private static final String DELETE_REPOSITORY = "" +
          "DELETE FROM MODULE_REPOSITORY WHERE REPOSITORY_ID = :REPOSITORY_ID;";

  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  public ModuleRepositoryDAO(final NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }


  @Nonnull
  private MapSqlParameterSource convertRepositoryToParameters(
          @Nonnull final Repository source) {
    Preconditions.checkNotNull(source);
    final MapSqlParameterSource parameters = new MapSqlParameterSource();
    try {
      parameters
              .addValue("REPOSITORY_ID", source.getId())
              .addValue("SNAPSHOT", source.isSnapshot())
              .addValue("URL", source.getUrl())
              .addValue("USERNAME", source.getUsername())
              .addValue("PASSWORD", source.getPassword())
              .addValue("PROXY_PROTOCOL",
                      source.getProxyProtocol() != null ? source.getProxyProtocol().name() : null)
              .addValue("PROXY_HOST", source.getProxyHost())
              .addValue("PROXY_PORT", source.getProxyPort())
              .addValue("PROXY_LOGIN", source.getProxyLogin())
              .addValue("PROXY_PASSWORD", source.getProxyPassword());
    } catch (final Exception e) {
      throw new RuntimeException("Fail to convert repository", e);
    }
    Preconditions.checkNotNull(parameters);
    return parameters;
  }

  @Nonnull
  private static Repository mapRow(final ResultSet resultSet, final int i) throws SQLException {
    return new Repository(
            resultSet.getBoolean("SNAPSHOT"),
            resultSet.getString("REPOSITORY_ID"),
            resultSet.getString("URL"),
            resultSet.getString("USERNAME"),
            resultSet.getString("PASSWORD"),
            Repository.ProxyProtocol.valueOf(resultSet.getString("PROXY_PROTOCOL")),
            resultSet.getString("PROXY_HOST"),
            resultSet.getInt("PROXY_PORT"),
            resultSet.getString("PROXY_LOGIN"),
            resultSet.getString("PROXY_PASSWORD")
    );
  }


  public List<Repository> getAll() {
    final SqlParameterSource parameters = new MapSqlParameterSource();

    return namedParameterJdbcTemplate.query(
            GET_ALL_REPOSITORIES,
            parameters,
            ModuleRepositoryDAO::mapRow);
  }

  @Nullable
  public Repository getById(@Nonnull final String id) {

    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("REPOSITORY_ID", id);

    return namedParameterJdbcTemplate.query(
            GET_BY_ID,
            parameters,
            ModuleRepositoryDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public void persist(@Nonnull final Repository repository) {
    namedParameterJdbcTemplate.update(PERSIST, convertRepositoryToParameters(repository));
  }

  public void delete(@Nonnull final String id) {
    namedParameterJdbcTemplate.update(DELETE_REPOSITORY, new MapSqlParameterSource("REPOSITORY_ID", id));
  }
}

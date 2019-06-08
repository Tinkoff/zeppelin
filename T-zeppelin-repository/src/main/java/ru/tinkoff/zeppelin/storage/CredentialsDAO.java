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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.Credential;

@Component
public class CredentialsDAO {

  private static final String PERSIST = "" +
      "INSERT INTO CREDENTIALS (KEY,\n" +
      "                         VALUE,\n" +
      "                         DESCRIPTION,\n" +
      "                         PERMISSIONS)\n" +
      "VALUES (:KEY,\n" +
      "        :VALUE,\n" +
      "        :DESCRIPTION,\n" +
      "        :PERMISSIONS);";

  private static final String UPDATE = "" +
      "UPDATE CREDENTIALS\n" +
      "SET KEY         = :KEY,\n" +
      "    VALUE       = :VALUE,\n" +
      "    DESCRIPTION = :DESCRIPTION,\n" +
      "    PERMISSIONS = :PERMISSIONS\n" +
      "WHERE ID = :ID;";

  private static final String GET_BY_ID = "" +
      "SELECT ID,\n" +
      "       KEY,\n" +
      "       VALUE,\n" +
      "       DESCRIPTION,\n" +
      "       PERMISSIONS\n" +
      "FROM CREDENTIALS\n" +
      "WHERE ID = :ID;";

  private static final String GET_BY_KEY = "" +
      "SELECT ID,\n" +
      "       KEY,\n" +
      "       VALUE,\n" +
      "       DESCRIPTION,\n" +
      "       PERMISSIONS\n" +
      "FROM CREDENTIALS\n" +
      "WHERE KEY = :KEY;";

  private static final String GET_ALL = "" +
      "SELECT ID,\n" +
      "       KEY,\n" +
      "       VALUE,\n" +
      "       DESCRIPTION,\n" +
      "       PERMISSIONS\n" +
      "FROM CREDENTIALS;";

  private static final String DELETE_BY_ID = "" +
      "DELETE FROM CREDENTIALS\n" +
      "WHERE ID = :ID;";

  private static final String DELETE_BY_KEY = "" +
      "DELETE FROM CREDENTIALS\n" +
      "WHERE KEY = :KEY;";

  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  public CredentialsDAO(final NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }

  @Nonnull
  public Credential persist(@Nonnull final Credential credential) {
    final HashMap<String, Set<String>> permission = new HashMap<>();
    permission.put("owners", credential.getOwners());
    permission.put("readers", credential.getReaders());

    final KeyHolder holder = new GeneratedKeyHolder();
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("KEY", credential.getKey())
        .addValue("VALUE", credential.getValue())
        .addValue("DESCRIPTION", credential.getDescription())
        .addValue("PERMISSIONS", generatePGjson(permission));

    namedParameterJdbcTemplate.update(PERSIST, parameters, holder);
    credential.setId((Long) holder.getKeys().get("id"));
    return credential;
  }

  @Nonnull
  public Credential update(@Nonnull final Credential credential) {
    final HashMap<String, Set<String>> permission = new HashMap<>();
    permission.put("owners", credential.getOwners());
    permission.put("readers", credential.getReaders());

    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("KEY", credential.getKey())
        .addValue("VALUE", credential.getValue())
        .addValue("DESCRIPTION", credential.getDescription())
        .addValue("PERMISSIONS", generatePGjson(permission))
        .addValue("ID", credential.getId());

    namedParameterJdbcTemplate.update(UPDATE, parameters);
    return credential;
  }

  @Nullable
  public Credential get(final long credentialId) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("ID", credentialId);

    return namedParameterJdbcTemplate.queryForObject(
        GET_BY_ID,
        parameters,
        CredentialsDAO::mapRow
    );
  }

  @Nullable
  public Credential get(@Nonnull final String key) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("KEY", key);

    return namedParameterJdbcTemplate.queryForObject(
        GET_BY_KEY,
        parameters,
        CredentialsDAO::mapRow
    );
  }

  @Nonnull
  public List<Credential> getAllCredentials() {
    final SqlParameterSource parameters = new MapSqlParameterSource();
    return namedParameterJdbcTemplate.query(
        GET_ALL,
        parameters,
        CredentialsDAO::mapRow);
  }

  public void remove(final long id) {
    namedParameterJdbcTemplate.update(
        DELETE_BY_ID,
        new MapSqlParameterSource("ID", id)
    );
  }

  public void remove(@Nonnull final String key) {
    namedParameterJdbcTemplate.update(
        DELETE_BY_KEY,
        new MapSqlParameterSource("KEY", key)
    );
  }

  @Nonnull
  private static Credential mapRow(final ResultSet resultSet, final int i) throws SQLException {
    final long id = resultSet.getLong("ID");
    final String key = resultSet.getString("KEY");
    final String value = resultSet.getString("VALUE");
    final String description = resultSet.getString("DESCRIPTION");

    Map<String, Set<String>> permission = new HashMap<>(2);
    permission = new Gson().fromJson(resultSet.getString("PERMISSIONS"), permission.getClass());

    final Credential credential = new Credential(key, value, description);
    credential.setId(id);
    credential.getOwners().addAll(permission.get("owners"));
    credential.getReaders().addAll(permission.get("readers"));

    return credential;
  }
}

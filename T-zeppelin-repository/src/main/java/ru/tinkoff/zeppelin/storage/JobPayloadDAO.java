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
import ru.tinkoff.zeppelin.core.notebook.JobPayload;

import java.sql.ResultSet;
import java.sql.SQLException;

@Component
public class JobPayloadDAO {

    private static final String PERSIST_PAYLOAD = "INSERT INTO JOB_PAYLOAD (JOB_ID, PAYLOAD) VALUES (:JOB_ID, :PAYLOAD);";

    private static final String LOAD_PAYLOAD_BY_ID = "SELECT * FROM JOB_PAYLOAD WHERE ID = :ID;";

    private static final String LOAD_PAYLOAD_BY_JOB_ID = "SELECT * FROM JOB_PAYLOAD WHERE JOB_ID = :JOB_ID;";

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public JobPayloadDAO(final NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    }

    public JobPayload persist(final JobPayload jobPayload) {
        final KeyHolder holder = new GeneratedKeyHolder();
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("JOB_ID", jobPayload.getJobId())
                .addValue("PAYLOAD", jobPayload.getPayload());
        namedParameterJdbcTemplate.update(PERSIST_PAYLOAD, parameters, holder);

        jobPayload.setId((Long) holder.getKeys().get("id"));
        return jobPayload;
    }

    private static JobPayload mapRow(final ResultSet resultSet, final int i) throws SQLException {
        final Long id = resultSet.getLong("ID");
        final Long jobId = resultSet.getLong("JOB_ID");
        final String payload = resultSet.getString("PAYLOAD");

        final JobPayload jobPayload = new JobPayload();
        jobPayload.setId(id);
        jobPayload.setJobId(jobId);
        jobPayload.setPayload(payload);
        return jobPayload;
    }

    public JobPayload get(final Long payloadId) {
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("ID", payloadId);

        return namedParameterJdbcTemplate.query(
                LOAD_PAYLOAD_BY_ID,
                parameters,
                JobPayloadDAO::mapRow)
                .stream()
                .findFirst()
                .orElse(null);
    }

    public JobPayload getByJobId(final Long identity) {
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("JOB_ID", identity);

        return namedParameterJdbcTemplate.query(
                LOAD_PAYLOAD_BY_JOB_ID,
                parameters,
                JobPayloadDAO::mapRow)
                .stream()
                .findFirst()
                .orElse(null);
    }
}

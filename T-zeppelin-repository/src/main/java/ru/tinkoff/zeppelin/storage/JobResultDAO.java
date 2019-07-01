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
import ru.tinkoff.zeppelin.core.notebook.JobResult;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

import static ru.tinkoff.zeppelin.storage.Utils.toTimestamp;

@Component
public class JobResultDAO {

    private static final String PERSIST_PAYLOAD = "" +
            "INSERT INTO JOB_RESULT (JOB_ID,\n" +
            "                        CREATED_AT,\n" +
            "                        TYPE,\n" +
            "                        RESULT)\n" +
            "VALUES (:JOB_ID,\n" +
            "        :CREATED_AT,\n" +
            "        :TYPE,\n" +
            "        :RESULT);";

    private static final String UPDATE_PAYLOAD = "" +
            "UPDATE JOB_RESULT\n" +
            "SET JOB_ID     = :JOB_ID,\n" +
            "    CREATED_AT = :CREATED_AT,\n" +
            "    TYPE       = :TYPE,\n" +
            "    RESULT     = :RESULT\n" +
            "WHERE ID = :ID;";

    private static final String DELETE_PAYLOAD = "" +
            "DELETE\n" +
            "FROM JOB_RESULT\n" +
            "WHERE ID = :ID;";

    private static final String LOAD_PAYLOAD_BY_ID = "SELECT ID,\n" +
            "       JOB_ID,\n" +
            "       CREATED_AT,\n" +
            "       TYPE,\n" +
            "       RESULT\n" +
            "FROM JOB_RESULT\n" +
            "WHERE ID = :ID;";

    private static final String LOAD_PAYLOAD_BY_JOB_ID = "SELECT ID,\n" +
            "       JOB_ID,\n" +
            "       CREATED_AT,\n" +
            "       TYPE,\n" +
            "       RESULT\n" +
            "FROM JOB_RESULT\n" +
            "WHERE JOB_ID = :JOB_ID;";

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public JobResultDAO(final NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    }

    private static JobResult mapRow(final ResultSet resultSet, final int i) throws SQLException {
        final Long id = resultSet.getLong("ID");
        final Long job_Id = resultSet.getLong("JOB_ID");
        final LocalDateTime created_at = resultSet.getTimestamp("CREATED_AT").toLocalDateTime();
        final String type = resultSet.getString("TYPE");
        final String result = resultSet.getString("RESULT");

        final JobResult jobResult = new JobResult();
        jobResult.setId(id);
        jobResult.setJobId(job_Id);
        jobResult.setCreatedAt(created_at);
        jobResult.setType(type);
        jobResult.setResult(result);
        return jobResult;
    }


    public JobResult persist(final JobResult jobResult) {
        final KeyHolder holder = new GeneratedKeyHolder();
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("JOB_ID", jobResult.getJobId())
                .addValue("CREATED_AT", toTimestamp((jobResult.getCreatedAt())))
                .addValue("TYPE", jobResult.getType())
                .addValue("RESULT", jobResult.getResult());
        namedParameterJdbcTemplate.update(PERSIST_PAYLOAD, parameters, holder);

        //noinspection ConstantConditions
        jobResult.setId((Long) holder.getKeys().get("id"));
        return jobResult;
    }

    public JobResult update(final JobResult jobResult) {
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("JOB_ID", jobResult.getJobId())
                .addValue("CREATED_AT", toTimestamp(jobResult.getCreatedAt()))
                .addValue("TYPE", jobResult.getType())
                .addValue("RESULT", jobResult.getResult())
                .addValue("ID", jobResult.getId());
        namedParameterJdbcTemplate.update(UPDATE_PAYLOAD, parameters);
        return jobResult;
    }

    public void delete(final long id) {
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("ID", id);
        namedParameterJdbcTemplate.update(DELETE_PAYLOAD, parameters);
    }

    public JobResult get(final Long jobResultId) {
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("ID", jobResultId);

        return namedParameterJdbcTemplate.query(
                LOAD_PAYLOAD_BY_ID,
                parameters,
                JobResultDAO::mapRow)
                .stream()
                .findFirst()
                .orElse(null);
    }

    public List<JobResult> getByJobId(final Long jobId) {
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("JOB_ID", jobId);

        return namedParameterJdbcTemplate.query(
                LOAD_PAYLOAD_BY_JOB_ID,
                parameters,
                JobResultDAO::mapRow
        );
    }


}

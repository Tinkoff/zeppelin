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

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.notebook.JobBatch;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

import static ru.tinkoff.zeppelin.storage.Utils.toTimestamp;

@Component
public class JobBatchDAO {

    private static final String PERSIST_BATCH = "INSERT INTO JOB_BATCH (NOTE_ID,\n" +
            "                       STATUS,\n" +
            "                       CREATED_AT,\n" +
            "                       STARTED_AT,\n" +
            "                       ENDED_AT)\n" +
            "            VALUES (:NOTE_ID,\n" +
            "                    :STATUS,\n" +
            "                    :CREATED_AT,\n" +
            "                    :STARTED_AT,\n" +
            "                    :ENDED_AT);";

    private static final String UPDATE_BATCH = "UPDATE JOB_BATCH\n" +
            "SET STATUS   = :STATUS,\n" +
            "  CREATED_AT = :CREATED_AT,\n" +
            "  STARTED_AT = :STARTED_AT,\n" +
            "  ENDED_AT   = :ENDED_AT\n" +
            "WHERE ID = :ID;";

    private static final String DELETE_BATCH = "" +
            "DELETE\n" +
            "FROM JOB_BATCH\n" +
            "WHERE ID = :ID;";

    private static final String LOAD_BATCH_BY_ID = "SELECT * FROM job_batch WHERE ID = :ID;";

    private static final String LOAD_BY_STATUS = "SELECT * FROM job_batch WHERE STATUS = :STATUS;";

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public JobBatchDAO(final NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    }

    private static JobBatch mapRow(final ResultSet resultSet, final int i) throws SQLException {
        final Long id = resultSet.getLong("ID");
        final Long noteId = resultSet.getLong("NOTE_ID");
        final JobBatch.Status status = JobBatch.Status.valueOf(resultSet.getString("STATUS"));

        final LocalDateTime createdAt =
                null != resultSet.getTimestamp("CREATED_AT")
                        ? resultSet.getTimestamp("CREATED_AT").toLocalDateTime()
                        : null;
        final LocalDateTime startedAt =
                null != resultSet.getTimestamp("STARTED_AT")
                        ? resultSet.getTimestamp("STARTED_AT").toLocalDateTime()
                        : null;

        final LocalDateTime endedAt =
                null != resultSet.getTimestamp("ENDED_AT")
                        ? resultSet.getTimestamp("ENDED_AT").toLocalDateTime()
                        : null;

        final JobBatch jobBatch = new JobBatch();
        jobBatch.setId(id);
        jobBatch.setNoteId(noteId);
        jobBatch.setStatus(status);
        jobBatch.setCreatedAt(createdAt);
        jobBatch.setStartedAt(startedAt);
        jobBatch.setEndedAt(endedAt);
        return jobBatch;
    }

    public JobBatch persist(final JobBatch jobBatch) {
        final KeyHolder holder = new GeneratedKeyHolder();
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("NOTE_ID", jobBatch.getNoteId())
                .addValue("STATUS", jobBatch.getStatus().name())
                .addValue("CREATED_AT", toTimestamp(jobBatch.getCreatedAt()))
                .addValue("STARTED_AT", toTimestamp(jobBatch.getStartedAt()))
                .addValue("ENDED_AT", toTimestamp(jobBatch.getEndedAt()));
        namedParameterJdbcTemplate.update(PERSIST_BATCH, parameters, holder);

        jobBatch.setId((Long) holder.getKeys().get("id"));
        return jobBatch;
    }

    public JobBatch update(final JobBatch jobBatch) {
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("NOTE_ID", jobBatch.getNoteId())
                .addValue("STATUS", jobBatch.getStatus().name())
                .addValue("CREATED_AT", toTimestamp(jobBatch.getCreatedAt()))
                .addValue("STARTED_AT", toTimestamp(jobBatch.getStartedAt()))
                .addValue("ENDED_AT", toTimestamp(jobBatch.getEndedAt()))
                .addValue("ID", jobBatch.getId());
        namedParameterJdbcTemplate.update(UPDATE_BATCH, parameters);
        return jobBatch;
    }

    public void delete(final long id) {
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("ID", id);
        namedParameterJdbcTemplate.update(DELETE_BATCH, parameters);
    }

    public JobBatch get(final Long batchId) {
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("ID", batchId);
        try {
            return namedParameterJdbcTemplate.queryForObject(LOAD_BATCH_BY_ID, parameters, JobBatchDAO::mapRow);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public List<JobBatch> getAborting() {
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("STATUS", JobBatch.Status.ABORTING.name());
        return namedParameterJdbcTemplate.query(LOAD_BY_STATUS, parameters, JobBatchDAO::mapRow);
    }
}

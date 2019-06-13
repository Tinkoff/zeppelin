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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.Set;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.notebook.Job;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

@Component
public class JobDAO {

  private static final String PERSIST_JOB = "" +
          "INSERT INTO JOB (BATCH_ID,\n" +
          "                 NOTE_ID,\n" +
          "                 PARAGRAPH_ID,\n" +
          "                 INDEX_NUMBER,\n" +
          "                 PRIORITY,\n" +
          "                 SHEBANG,\n" +
          "                 STATUS,\n" +
          "                 USER_NAME,\n" +
          "                 USER_ROLES,\n" +
          "                 INTERPRETER_PROCESS_UUID,\n" +
          "                 INTERPRETER_JOB_UUID,\n" +
          "                 CREATED_AT,\n" +
          "                 STARTED_AT,\n" +
          "                 ENDED_AT)\n" +
          "VALUES (:BATCH_ID,\n" +
          "        :NOTE_ID,\n" +
          "        :PARAGRAPH_ID,\n" +
          "        :INDEX_NUMBER,\n" +
          "        :PRIORITY,\n" +
          "        :SHEBANG,\n" +
          "        :STATUS,\n" +
          "        :USER_NAME,\n" +
          "        :USER_ROLES,\n" +
          "        :INTERPRETER_PROCESS_UUID,\n" +
          "        :INTERPRETER_JOB_UUID,\n" +
          "        :CREATED_AT,\n" +
          "        :STARTED_AT, :ENDED_AT);";

  private static final String UPDATE_JOB = "" +
          "UPDATE JOB\n" +
          "SET BATCH_ID                 = :BATCH_ID,\n" +
          "    NOTE_ID                  = :NOTE_ID,\n" +
          "    PARAGRAPH_ID             = :PARAGRAPH_ID,\n" +
          "    INDEX_NUMBER             = :INDEX_NUMBER,\n" +
          "    PRIORITY                 = :PRIORITY,\n" +
          "    SHEBANG                  = :SHEBANG,\n" +
          "    STATUS                   = :STATUS,\n" +
          "    USER_NAME                = :USER_NAME,\n" +
          "    USER_ROLES               = :USER_ROLES,\n" +
          "    INTERPRETER_PROCESS_UUID = :INTERPRETER_PROCESS_UUID,\n" +
          "    INTERPRETER_JOB_UUID     = :INTERPRETER_JOB_UUID,\n" +
          "    CREATED_AT               = :CREATED_AT,\n" +
          "    STARTED_AT               = :STARTED_AT,\n" +
          "    ENDED_AT                 = :ENDED_AT\n" +
          "WHERE ID = :ID;";

  private static final String SELECT_JOB = "" +
          "SELECT ID,\n" +
          "       BATCH_ID,\n" +
          "       NOTE_ID,\n" +
          "       PARAGRAPH_ID,\n" +
          "       INDEX_NUMBER,\n" +
          "       PRIORITY,\n" +
          "       SHEBANG,\n" +
          "       STATUS,\n" +
          "       USER_NAME,\n" +
          "       USER_ROLES,\n" +
          "       INTERPRETER_PROCESS_UUID,\n" +
          "       INTERPRETER_JOB_UUID,\n" +
          "       CREATED_AT,\n" +
          "       STARTED_AT,\n" +
          "       ENDED_AT\n" +
          "FROM JOB\n" +
          "WHERE ID = :ID;";

  private static final String SELECT_JOB_BY_INTERPRETER_JOB_UUID = "" +
          "SELECT ID,\n" +
          "       BATCH_ID,\n" +
          "       NOTE_ID,\n" +
          "       PARAGRAPH_ID,\n" +
          "       INDEX_NUMBER,\n" +
          "       PRIORITY,\n" +
          "       SHEBANG,\n" +
          "       STATUS,\n" +
          "       USER_NAME,\n" +
          "       USER_ROLES,\n" +
          "       INTERPRETER_PROCESS_UUID,\n" +
          "       INTERPRETER_JOB_UUID,\n" +
          "       CREATED_AT,\n" +
          "       STARTED_AT,\n" +
          "       ENDED_AT\n" +
          "FROM JOB\n" +
          "WHERE INTERPRETER_JOB_UUID = :INTERPRETER_JOB_UUID;";

  private static final String SELECT_JOBS_BY_BATCH = "" +
          "SELECT ID,\n" +
          "       BATCH_ID,\n" +
          "       NOTE_ID,\n" +
          "       PARAGRAPH_ID,\n" +
          "       INDEX_NUMBER,\n" +
          "       PRIORITY,\n" +
          "       SHEBANG,\n" +
          "       STATUS,\n" +
          "       USER_NAME,\n" +
          "       USER_ROLES,\n" +
          "       INTERPRETER_PROCESS_UUID,\n" +
          "       INTERPRETER_JOB_UUID,\n" +
          "       CREATED_AT,\n" +
          "       STARTED_AT,\n" +
          "       ENDED_AT\n" +
          "FROM JOB\n" +
          "WHERE BATCH_ID = :BATCH_ID" +
          " ORDER BY INDEX_NUMBER;";

  private static final String SELECT_READY_TO_EXECUTE_JOBS = "" +
          "SELECT DISTINCT ON (J.BATCH_ID)\n" +
          "  J.ID,\n" +
          "  J.BATCH_ID,\n" +
          "  J.NOTE_ID,\n" +
          "  J.PARAGRAPH_ID,\n" +
          "  J.INDEX_NUMBER,\n" +
          "  J.PRIORITY,\n" +
          "  J.SHEBANG,\n" +
          "  J.STATUS,\n" +
          "  J.USER_NAME,\n" +
          "  J.USER_ROLES,\n" +
          "  J.INTERPRETER_PROCESS_UUID,\n" +
          "  J.INTERPRETER_JOB_UUID,\n" +
          "  J.CREATED_AT,\n" +
          "  J.STARTED_AT,\n" +
          "  J.ENDED_AT\n" +
          "FROM JOB_BATCH JB\n" +
          "  LEFT JOIN JOB J ON JB.ID = J.BATCH_ID\n" +
          "WHERE JB.STATUS IN ('PENDING', 'RUNNING')\n" +
          "      AND J.STATUS != 'DONE'" +
          "      AND NOT EXISTS(SELECT * FROM JOB J2 WHERE J2.BATCH_ID = JB.ID AND J2.STATUS IN('RUNNING', 'ERROR'))\n" +
          "ORDER BY J.BATCH_ID, J.PRIORITY DESC, J.INDEX_NUMBER;";


  private static final String SELECT_JOBS_WITH_DEAD_INTERPRETER = "" +
          "SELECT J.ID,\n" +
          "       J.BATCH_ID,\n" +
          "       J.NOTE_ID,\n" +
          "       J.PARAGRAPH_ID,\n" +
          "       J.INDEX_NUMBER,\n" +
          "       J.PRIORITY,\n" +
          "       J.SHEBANG,\n" +
          "       J.STATUS,\n" +
          "       J.USER_NAME,\n" +
          "       J.USER_ROLES,\n" +
          "       J.INTERPRETER_PROCESS_UUID,\n" +
          "       J.INTERPRETER_JOB_UUID,\n" +
          "       J.CREATED_AT,\n" +
          "       J.STARTED_AT,\n" +
          "       J.ENDED_AT\n" +
          "FROM JOB_BATCH JB\n" +
          "       LEFT JOIN JOB J ON JB.ID = J.BATCH_ID\n" +
          "WHERE JB.STATUS IN ('PENDING', 'RUNNING')\n" +
          "  AND J.INTERPRETER_PROCESS_UUID = :INTERPRETER_PROCESS_UUID;";

  private static final String SELECT_READY_TO_CANCEL_JOBS = "" +
          "SELECT J.ID,\n" +
          "       J.BATCH_ID,\n" +
          "       J.NOTE_ID,\n" +
          "       J.PARAGRAPH_ID,\n" +
          "       J.INDEX_NUMBER,\n" +
          "       J.PRIORITY,\n" +
          "       J.SHEBANG,\n" +
          "       J.STATUS,\n" +
          "       J.USER_NAME,\n" +
          "       J.USER_ROLES,\n" +
          "       J.INTERPRETER_PROCESS_UUID,\n" +
          "       J.INTERPRETER_JOB_UUID,\n" +
          "       J.CREATED_AT,\n" +
          "       J.STARTED_AT,\n" +
          "       J.ENDED_AT\n" +
          "FROM JOB_BATCH JB\n" +
          "       LEFT JOIN JOB J ON JB.ID = J.BATCH_ID\n" +
          "WHERE JB.STATUS IN ('ABORTING')\n" +
          "  AND J.STATUS IN('RUNNING', 'PENDING', 'ABORTING');";

  private final static String RESTORE_JOBS_AFTER_SHUTDOWN = "" +
          "UPDATE JOB SET STATUS = 'PENDING',\n" +
          "               INTERPRETER_JOB_UUID = NULL,\n" +
          "               INTERPRETER_PROCESS_UUID = NULL\n" +
          "WHERE JOB.STATUS = 'RUNNING'\n" +
          "  AND INTERPRETER_PROCESS_UUID NOTNULL\n" +
          "  AND INTERPRETER_PROCESS_UUID NOT IN (:PROCESS_UUID)";


  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  public JobDAO(final NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }

  private static Job mapRow(final ResultSet resultSet, final int i) throws SQLException {
    Type rolesSetType = new TypeToken<Set<String>>() {
    }.getType();

    final Long id = resultSet.getLong("ID");
    final Long batch_id = resultSet.getLong("BATCH_ID");
    final Long noteId = resultSet.getLong("NOTE_ID");
    final Long paragraphId = resultSet.getLong("PARAGRAPH_ID");
    final Integer index = resultSet.getInt("INDEX_NUMBER");
    final Integer priority = resultSet.getInt("PRIORITY");
    final String shebang = resultSet.getString("SHEBANG");
    final Job.Status status = Job.Status.valueOf(resultSet.getString("STATUS"));
    final String username = resultSet.getString("USER_NAME");
    final Set<String> roles = new Gson().fromJson(resultSet.getString("USER_ROLES"), rolesSetType);
    final String interpreter_process_uuid = resultSet.getString("INTERPRETER_PROCESS_UUID");
    final String interpreter_job_uuid = resultSet.getString("INTERPRETER_JOB_UUID");

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

    final Job job = new Job();
    job.setId(id);
    job.setBatchId(batch_id);
    job.setNoteId(noteId);
    job.setParagraphId(paragraphId);
    job.setIndex(index);
    job.setPriority(priority);
    job.setShebang(shebang);
    job.setStatus(status);
    job.setUsername(username);
    job.setRoles(roles);
    job.setInterpreterProcessUUID(interpreter_process_uuid);
    job.setInterpreterJobUUID(interpreter_job_uuid);
    job.setCreatedAt(createdAt);
    job.setStartedAt(startedAt);
    job.setEndedAt(endedAt);
    return job;
  }

  public Job persist(final Job job) {
    final KeyHolder holder = new GeneratedKeyHolder();
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("BATCH_ID", job.getBatchId())
            .addValue("NOTE_ID", job.getNoteId())
            .addValue("PARAGRAPH_ID", job.getParagraphId())
            .addValue("INDEX_NUMBER", job.getIndex())
            .addValue("PRIORITY", job.getPriority())
            .addValue("SHEBANG", job.getShebang())
            .addValue("STATUS", job.getStatus().name())
            .addValue("USER_NAME", job.getUsername())
            .addValue("USER_ROLES", new Gson().toJson(job.getRoles()))
            .addValue("INTERPRETER_PROCESS_UUID", job.getInterpreterProcessUUID())
            .addValue("INTERPRETER_JOB_UUID", job.getInterpreterJobUUID())
            .addValue("CREATED_AT", job.getCreatedAt())
            .addValue("STARTED_AT", job.getStartedAt())
            .addValue("ENDED_AT", job.getEndedAt());
    namedParameterJdbcTemplate.update(PERSIST_JOB, parameters, holder);

    job.setId((Long) holder.getKeys().get("id"));
    return job;
  }

  public Job update(final Job job) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("BATCH_ID", job.getBatchId())
            .addValue("NOTE_ID", job.getNoteId())
            .addValue("PARAGRAPH_ID", job.getParagraphId())
            .addValue("INDEX_NUMBER", job.getIndex())
            .addValue("PRIORITY", job.getPriority())
            .addValue("SHEBANG", job.getShebang())
            .addValue("STATUS", job.getStatus().name())
            .addValue("USER_NAME", job.getUsername())
            .addValue("USER_ROLES", new Gson().toJson(job.getRoles()))
            .addValue("INTERPRETER_PROCESS_UUID", job.getInterpreterProcessUUID())
            .addValue("INTERPRETER_JOB_UUID", job.getInterpreterJobUUID())
            .addValue("CREATED_AT", job.getCreatedAt())
            .addValue("STARTED_AT", job.getStartedAt())
            .addValue("ENDED_AT", job.getEndedAt())
            .addValue("ID", job.getId());
    namedParameterJdbcTemplate.update(UPDATE_JOB, parameters);
    return job;
  }

  public Job get(final Long jobId) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", jobId);

    return namedParameterJdbcTemplate.query(
            SELECT_JOB,
            parameters,
            JobDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public Job getByInterpreterJobUUID(final String interpreterJobUUID) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("INTERPRETER_JOB_UUID", interpreterJobUUID);

    return namedParameterJdbcTemplate.query(
            SELECT_JOB_BY_INTERPRETER_JOB_UUID,
            parameters,
            JobDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public LinkedList<Job> loadNextPending() {
    final LinkedList<Job> jobs = new LinkedList<>(namedParameterJdbcTemplate.query(
            SELECT_READY_TO_EXECUTE_JOBS,
            JobDAO::mapRow));
    // TODO: KOT LOOK AT THIS
    jobs.sort(Comparator.comparing(Job::getIndex));
    return jobs;
  }

  public List<Job> loadByBatch(final Long batchId) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("BATCH_ID", batchId);

    return namedParameterJdbcTemplate.query(
            SELECT_JOBS_BY_BATCH,
            parameters,
            JobDAO::mapRow);
  }

  public List<Job> loadJobsByInterpreterProcessUUID(final String interpreterProcessUUID) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("INTERPRETER_PROCESS_UUID", interpreterProcessUUID);

    return namedParameterJdbcTemplate.query(
            SELECT_JOBS_WITH_DEAD_INTERPRETER,
            parameters,
            JobDAO::mapRow);
  }

  public List<Job> loadNextCancelling() {
    final SqlParameterSource parameters = new MapSqlParameterSource();

    return namedParameterJdbcTemplate.query(
            SELECT_READY_TO_CANCEL_JOBS,
            parameters,
            JobDAO::mapRow);
  }

  public void restoreState(final List<String> processUUID) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("PROCESS_UUID", processUUID);
    namedParameterJdbcTemplate.update(
            RESTORE_JOBS_AFTER_SHUTDOWN,
            parameters);
  }
}

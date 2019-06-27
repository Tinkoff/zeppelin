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
package ru.tinkoff.zeppelin.engine.handler;

import javax.annotation.PostConstruct;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.SystemEvent;
import ru.tinkoff.zeppelin.core.content.Content;
import ru.tinkoff.zeppelin.core.content.ContentType;
import ru.tinkoff.zeppelin.core.notebook.Job;
import ru.tinkoff.zeppelin.core.notebook.JobBatch;
import ru.tinkoff.zeppelin.core.notebook.JobPriority;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.engine.NoteEventService;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.PredefinedInterpreterResults;
import ru.tinkoff.zeppelin.storage.*;
import ru.tinkoff.zeppelin.storage.SystemEventType.ET;

import java.io.File;
import java.nio.file.Paths;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Class for handle intepreter results
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
@Component
public class InterpreterResultHandler extends AbstractHandler {
  private static final Logger LOG = LoggerFactory.getLogger(InterpreterResultHandler.class);


  private final ApplicationContext applicationContext;
  private final ContentDAO contentDAO;
  private final ContentParamDAO contentParamDAO;

  private static InterpreterResultHandler instance;

  public static InterpreterResultHandler getInstance() {
    return instance;
  }

  public InterpreterResultHandler(final JobBatchDAO jobBatchDAO,
                                  final JobDAO jobDAO,
                                  final JobResultDAO jobResultDAO,
                                  final JobPayloadDAO jobPayloadDAO,
                                  final NoteDAO noteDAO,
                                  final ParagraphDAO paragraphDAO,
                                  final FullParagraphDAO fullParagraphDAO,
                                  final ApplicationContext applicationContext,
                                  final NoteEventService noteEventService,
                                  final ContentDAO contentDAO,
                                  final ContentParamDAO contentParamDAO) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO, noteEventService);
    this.applicationContext = applicationContext;
    this.contentDAO = contentDAO;
    this.contentParamDAO = contentParamDAO;
  }

  @PostConstruct
  private void init() {
    instance = applicationContext.getBean(InterpreterResultHandler.class);
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handleResult(final String interpreterJobUUID,
                           final InterpreterResult interpreterResult) {

    final Job job = getWithTimeout(interpreterJobUUID);

    if (job == null) {
      ZLog.log(ET.JOB_NOT_FOUND, "Задача не найдена, interpreterJobUUID=" + interpreterJobUUID,
              SystemEvent.SYSTEM_USERNAME);
      return;
    }

    // clear appended results
    removeTempOutput(job);

    final JobBatch batch = jobBatchDAO.get(job.getBatchId());
    ZLog.log(ET.GOT_JOB,
            String.format("Получен батч[status=%s, id=%s] для задачи[id=%s, noteId=%s, paragraphId=%s]",
                    batch.getStatus(), batch.getId(), job.getId(), job.getNoteId(), job.getParagraphId()),
            SystemEvent.SYSTEM_USERNAME);

    if (batch.getStatus() == JobBatch.Status.ABORTING || batch.getStatus() == JobBatch.Status.ABORTED) {
      ZLog.log(ET.GOT_ABORTED_BATCH, "Батч находится в статусе " + batch.getStatus(), SystemEvent.SYSTEM_USERNAME);
      setAbortResult(job, batch, interpreterResult);
      return;
    }

    if (interpreterResult == null) {
      ZLog.log(ET.INTERPRETER_RESULT_NOT_FOUND,
              "Полученный результат равен \"null\", interpreterJobUUID=" + interpreterJobUUID,
              SystemEvent.SYSTEM_USERNAME
      );

      setErrorResult(job, batch, PredefinedInterpreterResults.ERROR_WHILE_INTERPRET);
      return;
    }
    scanH2(job.getNoteId());

    switch (interpreterResult.code()) {
      case SUCCESS:
        ZLog.log(ET.SUCCESSFUL_RESULT,
                "Задача успешно выполнена interpreterJobUUID=" + interpreterJobUUID,
                SystemEvent.SYSTEM_USERNAME
        );

        setSuccessResult(job, batch, interpreterResult);
        break;
      case ABORTED:
        ZLog.log(ET.ABORTED_RESULT, "Задача отменена interpreterJobUUID=%s" + interpreterJobUUID,
                SystemEvent.SYSTEM_USERNAME);
        setAbortResult(job, batch, interpreterResult);
        break;
      case ERROR:
        ZLog.log(ET.ERRORED_RESULT, "Задача завершена с ошибкой interpreterJobUUID=" + interpreterJobUUID,
                SystemEvent.SYSTEM_USERNAME);
        if (job.getPriority() == JobPriority.SCHEDULER.getIndex()) {
          noteEventService.errorOnNoteScheduleExecution(job);
        }
        setErrorResult(job, batch, interpreterResult);
        break;
    }
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handleTempOutput(final String interpreterJobUUID,
                               final String append) {

    final Job job = getWithTimeout(interpreterJobUUID);
    if (job == null) {
      ZLog.log(ET.JOB_NOT_FOUND, "Job not found by uuid=" + interpreterJobUUID,
              "Job not found by uuid=" + interpreterJobUUID, "Unknown");
      return;
    }

    if (job.getStatus() != Job.Status.RUNNING) {
      return;
    }
    publishTempOutput(job, append);
  }

  private Job getWithTimeout(final String interpreterJobUUID) {
    Job job = null;
    // задержка на закрытие транзакций
    for (int i = 0; i < 2 * 10 * 60; i++) {
      job = jobDAO.getByInterpreterJobUUID(interpreterJobUUID);
      if (job != null) {
        break;
      }
      try {
        Thread.sleep(100);
      } catch (final Exception e) {
        // SKIp
      }
    }
    return job;
  }

  private void scanH2(final long noteId) {
    final String noteContextPath =
            Configuration.getNoteStorePath()
                    + File.separator
                    + noteDAO.get(noteId).getUuid()
                    + File.separator
                    + "outputDB";
    try (final Connection con = DriverManager.getConnection(
            "jdbc:h2:file:" + Paths.get(noteContextPath).normalize().toFile().getAbsolutePath(),
            "sa",
            "")) {

      compareH2ToContext(con, noteContextPath, noteId);
    } catch (final Throwable th) {
      //SKIP
    }
    //SKIP
  }

  private void compareH2ToContext(final Connection connection, final String locationBase, final long noteId) throws SQLException, NullPointerException {
    final ResultSet schemas = connection.createStatement()
            .executeQuery("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME != 'INFORMATION_SCHEMA';");
    while (schemas.next()) {
      final String schema = schemas.getString("SCHEMA_NAME");
      final ResultSet resultSet = connection
              .createStatement()
              .executeQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema + "';");
      final LinkedList<String> tables = new LinkedList<>();
      while (resultSet.next()) {
        tables.add(resultSet.getString("TABLE_NAME"));
      }

      final List<Content> contentList = contentDAO.getNoteContent(noteId);

      for (final String tableName : tables) {
        final String location = locationBase + ":" + schema + "." + tableName;
        final ResultSet tableResultSet = connection.createStatement().executeQuery(
                "SELECT *  FROM " + tableName + ";");

        final ResultSetMetaData md = tableResultSet.getMetaData();

        final List<String> columns = new LinkedList<>();

        for (int i = 1; i < md.getColumnCount() + 1; i++) {

          final String createTable = (StringUtils.isNotEmpty(md.getColumnLabel(i))
                  ? md.getColumnLabel(i)
                  : md.getColumnName(i)) +
                  " \t" +
                  md.getColumnTypeName(i);
          columns.add(createTable);
        }
        contentList.stream()
                .filter(j -> location.equals(j.getLocation()))
                .findFirst().ifPresent(contentDAO::remove);
        try {
          contentDAO.persist(new Content(noteId, ContentType.TABLE, "rows", location, null));
          final Content content = contentDAO.getContentByLocation(location);
          if (content != null) {
            contentParamDAO.persist(content.getId(), "TABLE_COLUMNS", new Gson().fromJson(new Gson().toJson(columns), JsonElement.class));
          }
        } catch (final Exception ex) {
          LOG.info(ex.getMessage());
        }
      }
    }
  }
}
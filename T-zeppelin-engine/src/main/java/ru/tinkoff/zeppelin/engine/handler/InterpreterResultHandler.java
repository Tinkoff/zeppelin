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

import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.SystemEvent;
import ru.tinkoff.zeppelin.core.notebook.Job;
import ru.tinkoff.zeppelin.core.notebook.JobBatch;
import ru.tinkoff.zeppelin.core.notebook.JobPriority;
import ru.tinkoff.zeppelin.engine.NoteEventService;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.PredefinedInterpreterResults;
import ru.tinkoff.zeppelin.storage.FullParagraphDAO;
import ru.tinkoff.zeppelin.storage.JobBatchDAO;
import ru.tinkoff.zeppelin.storage.JobDAO;
import ru.tinkoff.zeppelin.storage.JobPayloadDAO;
import ru.tinkoff.zeppelin.storage.JobResultDAO;
import ru.tinkoff.zeppelin.storage.NoteDAO;
import ru.tinkoff.zeppelin.storage.ParagraphDAO;
import ru.tinkoff.zeppelin.storage.SystemEventType.ET;
import ru.tinkoff.zeppelin.storage.ZLog;

/**
 * Class for handle intepreter results
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
@Component
public class InterpreterResultHandler extends AbstractHandler {

  private ApplicationContext applicationContext;
  private final NoteEventService noteEventService;

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
                                  final NoteEventService noteEventService) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO);
    this.applicationContext = applicationContext;
    this.noteEventService = noteEventService;
  }

  @PostConstruct
  private void init() {
    instance = applicationContext.getBean(InterpreterResultHandler.class);
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handleResult(final String interpreterJobUUID,
                           final InterpreterResult interpreterResult) {

    Job job = getWithTimeout(interpreterJobUUID);

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
        if ( job.getPriority() == JobPriority.SCHEDULER.getIndex()){
          noteEventService.errorOnNoteScheduleExecution(job);
        }
        setErrorResult(job, batch, interpreterResult);
        break;
    }
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handleTempOutput(final String interpreterJobUUID,
                               final String append) {

    Job job = getWithTimeout(interpreterJobUUID);
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
}

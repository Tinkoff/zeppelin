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

import com.google.gson.Gson;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.core.notebook.*;
import ru.tinkoff.zeppelin.engine.NoteEventService;
import ru.tinkoff.zeppelin.engine.server.AbstractRemoteProcess;
import ru.tinkoff.zeppelin.engine.server.InterpreterRemoteProcess;
import ru.tinkoff.zeppelin.engine.server.RemoteProcessType;
import ru.tinkoff.zeppelin.interpreter.Context;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.PredefinedInterpreterResults;
import ru.tinkoff.zeppelin.storage.*;

import javax.annotation.PostConstruct;
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

  private final ApplicationContext applicationContext;

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
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO, noteEventService);
    this.applicationContext = applicationContext;
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
      return;
    }

    // clear appended results
    removeTempOutput(job);

    final JobBatch batch = jobBatchDAO.get(job.getBatchId());
    if (batch.getStatus() == JobBatch.Status.ABORTING || batch.getStatus() == JobBatch.Status.ABORTED) {
      setAbortResult(job, batch, interpreterResult);
      return;
    }

    if (interpreterResult == null) {
      setErrorResult(job, batch, PredefinedInterpreterResults.ERROR_WHILE_INTERPRET);
      return;
    }

    switch (interpreterResult.code()) {
      case SUCCESS:
        setSuccessResult(job, batch, interpreterResult);
        if (batch.getStatus() == JobBatch.Status.DONE && job.getPriority() == JobPriority.SCHEDULER.getIndex()) {
          noteEventService.successNoteScheduleExecution(batch.getNoteId());
        }
        break;
      case ABORTED:
        setAbortResult(job, batch, interpreterResult);
        break;
      case ERROR:
        if (job.getPriority() == JobPriority.SCHEDULER.getIndex()) {
          noteEventService.errorOnNoteScheduleExecution(job);
        }
        setErrorResult(job, batch, interpreterResult);
        break;
    }

    if (!JobBatch.Status.getRunningStatuses().contains(batch.getStatus())) {
      // работа закончена, сигналим интерпретаторам
      final Note note = noteDAO.get(batch.getNoteId());
      final List<Job> jobs = jobDAO.loadByBatch(batch.getId());
      for (final Job iterateJob : jobs) {
        try {
          final AbstractRemoteProcess process =
              iterateJob.getShebang() != null
                  ? AbstractRemoteProcess.get(iterateJob.getShebang(), RemoteProcessType.INTERPRETER)
                  : null;

          if (process != null && process.getStatus() == AbstractRemoteProcess.Status.READY) {
            final Paragraph paragraph = paragraphDAO.get(iterateJob.getParagraphId());
            final Context context = new Context(
                note.getId(),
                note.getUuid(),
                paragraph.getId(),
                paragraph.getUuid(),
                iterateJob.getPriority() == JobPriority.SCHEDULER.getIndex()
                    ? Context.StartType.SCHEDULED
                    : Context.StartType.REGULAR

            );
            ((InterpreterRemoteProcess) process).finish(new Gson().toJson(context));
          }
        } catch (final Throwable th) {
          // SKIP
        }
      }
    }
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handleTempOutput(final String interpreterJobUUID,
                               final String append) {

    final Job job = getWithTimeout(interpreterJobUUID);
    if (job == null) {
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

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

import com.google.common.collect.Sets;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.SystemEvent;
import ru.tinkoff.zeppelin.core.notebook.Job;
import ru.tinkoff.zeppelin.core.notebook.JobBatch;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.engine.NoteEventService;
import ru.tinkoff.zeppelin.engine.server.AbstractRemoteProcess;
import ru.tinkoff.zeppelin.engine.server.InterpreterRemoteProcess;
import ru.tinkoff.zeppelin.engine.server.RemoteProcessType;
import ru.tinkoff.zeppelin.interpreter.PredefinedInterpreterResults;
import ru.tinkoff.zeppelin.interpreter.thrift.CancelResult;
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
 * Class for handle abort state of jobs
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
@Component
public class AbortHandler extends AbstractHandler {

  public AbortHandler(final JobBatchDAO jobBatchDAO,
                      final JobDAO jobDAO,
                      final JobResultDAO jobResultDAO,
                      final JobPayloadDAO jobPayloadDAO,
                      final NoteDAO noteDAO,
                      final ParagraphDAO paragraphDAO,
                      final FullParagraphDAO fullParagraphDAO,
                      final NoteEventService noteEventService) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO, noteEventService);
  }

  public List<JobBatch> loadJobs() {
    return jobBatchDAO.getAborting();
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handle(final JobBatch batch) {

    final List<Job> jobs = jobDAO.loadByBatch(batch.getId());
    final Set<Job.Status> runningStatuses = Sets.newHashSet(Job.Status.RUNNING, Job.Status.ABORTING);
    final boolean hasRunningJob = jobs.stream().anyMatch(j -> runningStatuses.contains(j.getStatus()));
    if(! hasRunningJob) {
      setFailedResult(null, null, batch, JobBatch.Status.ABORTED, PredefinedInterpreterResults.OPERATION_ABORTED);
      return;
    }

    jobs.stream()
            .filter(j -> runningStatuses.contains(j.getStatus()))
            .forEach(j -> abortRunningJob(batch, j));

  }

  private void abortRunningJob(final JobBatch batch, final Job job) {
    final InterpreterRemoteProcess remote = (InterpreterRemoteProcess) AbstractRemoteProcess.get(job.getShebang(), RemoteProcessType.INTERPRETER);
    if (remote == null) {
      setAbortResult(job, batch, PredefinedInterpreterResults.OPERATION_ABORTED);
      ZLog.log(
          ET.INTERPRETER_PROCESS_NOT_FOUND,
          String.format("Процесс интрепретатора не найден, shebang: %s", job.getShebang()),
          String.format("Ошибка в ходе отмены задачи: не найден процесс для существующей задачи: job[%s]", job.toString()),
          SystemEvent.SYSTEM_USERNAME
      );
      return;
    }

    CancelResult cancelResult = null;
    try {
      cancelResult = remote.cancel(job.getInterpreterJobUUID());
      Objects.requireNonNull(cancelResult);

    } catch (final Exception e) {
      setAbortResult(job, batch, PredefinedInterpreterResults.OPERATION_ABORTED);
      ZLog.log(
          ET.JOB_CANCEL_FAILED,
          String.format("Ошибка в ходе отмены задачи с uuid: %s", job.getInterpreterJobUUID()),
          String.format("В ходе отмены задачи было брошено исключение: %s", ExceptionUtils.getStackTrace(e)),
          SystemEvent.SYSTEM_USERNAME
      );
      return;
    }

    switch (cancelResult.status) {
      case ACCEPT:
        ZLog.log(
            ET.JOB_CANCEL_ACCEPTED,
            String.format("Задача перешла в статус ABORTING: job[id=%s]", job.getId()),
            String.format("Сигнал отмены был отправлен процессу интерпретатора: process[%s]", remote.toString()),
            SystemEvent.SYSTEM_USERNAME
        );
        setFailedResult(job, Job.Status.ABORTING, null, null, null);
        break;
      case NOT_FOUND:
        ZLog.log(
            ET.JOB_CANCEL_NOT_FOUND,
            String.format("Не найден процесс, который необходимо отменить: job[id=%s]", job.getId()),
            String.format("Статус CancelResult - \"not found\": process[%s]", remote.toString()),
            SystemEvent.SYSTEM_USERNAME
        );
        setAbortResult(job, batch, PredefinedInterpreterResults.OPERATION_ABORTED);
        break;
      case ERROR:
      default:
        ZLog.log(
            ET.JOB_CANCEL_ERRORED,
            String.format("Ошибка в ходе отмены задачи job[id=%s]", job.getId()),
            String.format("Статус CancelResult - \"error\": process[%s]", remote.toString()),
            SystemEvent.SYSTEM_USERNAME
        );
        setAbortResult(job, batch, PredefinedInterpreterResults.OPERATION_ABORTED);
    }
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void abort(final Note note) {
    abortingBatch(note);
  }
}

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

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import ru.tinkoff.zeppelin.core.externalDTO.ParagraphDTO;
import ru.tinkoff.zeppelin.core.notebook.Job;
import ru.tinkoff.zeppelin.core.notebook.JobBatch;
import ru.tinkoff.zeppelin.core.notebook.JobBatch.Status;
import ru.tinkoff.zeppelin.core.notebook.JobPayload;
import ru.tinkoff.zeppelin.core.notebook.JobResult;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.EventService;
import ru.tinkoff.zeppelin.engine.forms.FormsProcessor;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.storage.FullParagraphDAO;
import ru.tinkoff.zeppelin.storage.JobBatchDAO;
import ru.tinkoff.zeppelin.storage.JobDAO;
import ru.tinkoff.zeppelin.storage.JobPayloadDAO;
import ru.tinkoff.zeppelin.storage.JobResultDAO;
import ru.tinkoff.zeppelin.storage.NoteDAO;
import ru.tinkoff.zeppelin.storage.ParagraphDAO;

/**
 * Base class for handlers
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
abstract class AbstractHandler {

  final JobBatchDAO jobBatchDAO;
  final JobDAO jobDAO;
  private final JobResultDAO jobResultDAO;
  final JobPayloadDAO jobPayloadDAO;
  final NoteDAO noteDAO;
  final ParagraphDAO paragraphDAO;
  private final FullParagraphDAO fullParagraphDAO;

  public AbstractHandler(final JobBatchDAO jobBatchDAO,
                         final JobDAO jobDAO,
                         final JobResultDAO jobResultDAO,
                         final JobPayloadDAO jobPayloadDAO,
                         final NoteDAO noteDAO,
                         final ParagraphDAO paragraphDAO,
                         final FullParagraphDAO fullParagraphDAO) {
    this.jobBatchDAO = jobBatchDAO;
    this.jobDAO = jobDAO;
    this.jobResultDAO = jobResultDAO;
    this.jobPayloadDAO = jobPayloadDAO;
    this.noteDAO = noteDAO;
    this.paragraphDAO = paragraphDAO;
    this.fullParagraphDAO = fullParagraphDAO;
  }

  void setRunningState(final Job job,
                       final String interpreterProcessUUID,
                       final String interpreterJobUUID) {

    final ParagraphDTO before = fullParagraphDAO.getById(job.getParagraphId());

    job.setStatus(Job.Status.RUNNING);
    job.setStartedAt(LocalDateTime.now());
    job.setInterpreterProcessUUID(interpreterProcessUUID);
    job.setInterpreterJobUUID(interpreterJobUUID);
    jobDAO.update(job);

    final ParagraphDTO after = fullParagraphDAO.getById(job.getParagraphId());
    EventService.publish(job.getNoteId(), before, after);

    final JobBatch jobBatch = jobBatchDAO.get(job.getBatchId());
    if (jobBatch.getStatus() == JobBatch.Status.PENDING) {
      jobBatch.setStatus(JobBatch.Status.RUNNING);
      jobBatch.setStartedAt(LocalDateTime.now());
      jobBatchDAO.update(jobBatch);
    }
  }

  void setSuccessResult(final Job job,
                        final JobBatch batch,
                        final InterpreterResult interpreterResult) {

    final ParagraphDTO before = fullParagraphDAO.getById(job.getParagraphId());

    persistMessages(job, interpreterResult.message());

    job.setStatus(Job.Status.DONE);
    job.setEndedAt(LocalDateTime.now());
    job.setInterpreterJobUUID(null);
    job.setInterpreterProcessUUID(null);
    jobDAO.update(job);

    final ParagraphDTO after = fullParagraphDAO.getById(job.getParagraphId());
    EventService.publish(job.getNoteId(), before, after);

    final List<Job> jobs = jobDAO.loadByBatch(job.getBatchId());
    final boolean isDone = jobs.stream().noneMatch(j -> j.getStatus() != Job.Status.DONE);
    if (isDone) {
      batch.setStatus(JobBatch.Status.DONE);
      batch.setEndedAt(LocalDateTime.now());
      jobBatchDAO.update(batch);
    }
  }


  void setErrorResult(final Job job,
                      final JobBatch batch,
                      final InterpreterResult interpreterResult) {

    setFailedResult(job, Job.Status.ERROR, batch, JobBatch.Status.ERROR, interpreterResult);
  }

  void setAbortResult(final Job job,
                      final JobBatch batch,
                      final InterpreterResult interpreterResult) {

    setFailedResult(job, Job.Status.ABORTED, batch, JobBatch.Status.ABORTED, interpreterResult);
  }

  void setFailedResult(final Job job,
                       final Job.Status jobStatus,
                       final JobBatch batch,
                       final JobBatch.Status jobBatchStatus,
                       final InterpreterResult interpreterResult) {

    if (job != null) {
      final ParagraphDTO before = fullParagraphDAO.getById(job.getParagraphId());

      if (interpreterResult != null) {
        persistMessages(job, interpreterResult.message());
      }

      job.setStatus(jobStatus);
      job.setEndedAt(LocalDateTime.now());
      jobDAO.update(job);

      final ParagraphDTO after = fullParagraphDAO.getById(job.getParagraphId());
      EventService.publish(job.getNoteId(), before, after);
    }

    if (batch != null) {
      final List<Job> jobs = jobDAO.loadByBatch(batch.getId());
      for (final Job j : jobs) {
        final ParagraphDTO beforeInner = fullParagraphDAO.getById(j.getParagraphId());

        if (j.getStatus() == Job.Status.PENDING) {
          j.setStatus(Job.Status.CANCELED);
          j.setStartedAt(LocalDateTime.now());
          j.setEndedAt(LocalDateTime.now());
        }
        j.setInterpreterJobUUID(null);
        j.setInterpreterProcessUUID(null);
        jobDAO.update(j);

        final ParagraphDTO afterInner = fullParagraphDAO.getById(j.getParagraphId());
        EventService.publish(j.getNoteId(), beforeInner, afterInner);
      }

      batch.setStatus(jobBatchStatus);
      batch.setEndedAt(LocalDateTime.now());
      jobBatchDAO.update(batch);
    }
  }

  private void persistMessages(final Job job,
                               final List<InterpreterResult.Message> messages) {

    for (final InterpreterResult.Message message : messages) {
      final JobResult jobResult = new JobResult();
      jobResult.setJobId(job.getId());
      jobResult.setCreatedAt(LocalDateTime.now());
      jobResult.setType(message.getType().name());
      jobResult.setResult(message.getData());
      jobResultDAO.persist(jobResult);
    }
  }

  long publishBatch(
          final Note note,
          final List<Paragraph> paragraphs,
          final String username,
          final Set<String> roles,
          final int priority) {
    final JobBatch batch = new JobBatch();
    batch.setId(0L);
    batch.setNoteId(note.getId());
    batch.setStatus(JobBatch.Status.SAVING);
    batch.setCreatedAt(LocalDateTime.now());
    batch.setStartedAt(null);
    batch.setEndedAt(null);
    final JobBatch saved = jobBatchDAO.persist(batch);

    boolean hasParagraphToExecute = false;
    for (int i = 0; i < paragraphs.size(); i++) {
      final Paragraph p = paragraphs.get(i);

      if (!(boolean) p.getConfig().getOrDefault("enabled", true)) {
        continue;
      }

      final ParagraphDTO before = fullParagraphDAO.getById(p.getId());

      final Job job = new Job();
      job.setId(0L);
      job.setBatchId(saved.getId());
      job.setNoteId(note.getId());
      job.setParagraphId(p.getId());
      job.setIndex(i);
      job.setPriority(priority);
      job.setShebang(p.getShebang());
      job.setStatus(Job.Status.PENDING);
      job.setInterpreterProcessUUID(null);
      job.setInterpreterJobUUID(null);
      job.setCreatedAt(LocalDateTime.now());
      job.setStartedAt(null);
      job.setEndedAt(null);
      job.setUsername(username);
      job.setRoles(roles);
      jobDAO.persist(job);

      final JobPayload jobPayload = new JobPayload();
      jobPayload.setId(0L);
      jobPayload.setJobId(job.getId());
      String payload = StringUtils.firstNonEmpty(p.getSelectedText(), p.getText());
      jobPayload.setPayload(FormsProcessor.injectFormValues(payload, p.getFormParams()));
      jobPayloadDAO.persist(jobPayload);

      p.setJobId(job.getId());
      paragraphDAO.update(p);

      final ParagraphDTO after = fullParagraphDAO.getById(job.getParagraphId());
      EventService.publish(job.getNoteId(), before, after);
      hasParagraphToExecute = true;
    }

    // in case of empty note
    // delete batch and return
    if (!hasParagraphToExecute) {
      jobBatchDAO.delete(saved.getId());
      return -1;
    }

    saved.setStatus(JobBatch.Status.PENDING);
    jobBatchDAO.update(saved);
    note.setBatchJobId(batch.getId());
    noteDAO.update(note);

    return saved.getId();
  }

  void abortingBatch(final Note note) {
    final JobBatch jobBatch = jobBatchDAO.get(note.getBatchJobId());
    jobBatch.setStatus(JobBatch.Status.ABORTING);
    jobBatchDAO.update(jobBatch);
  }

  boolean noteIsRunning(final Note note) {
    JobBatch jobBatch = jobBatchDAO.get(note.getBatchJobId());
    if (jobBatch == null) {
      return false;
    }
    Status status = jobBatch.getStatus();
    return Status.running.contains(status);
  }

  void publishTempOutput(final Job job, final String tempText) {
    final ParagraphDTO before = fullParagraphDAO.getById(job.getParagraphId());

    final List<JobResult> results = jobResultDAO.getByJobId(job.getId()).stream()
            .filter(j -> InterpreterResult.Message.Type.TEXT_TEMP.name().equals(j.getType()))
            .collect(Collectors.toList());

    if(results.isEmpty()) {
      final JobResult jobResult = new JobResult();
      jobResult.setJobId(job.getId());
      jobResult.setCreatedAt(LocalDateTime.now());
      jobResult.setType(InterpreterResult.Message.Type.TEXT_TEMP.name());
      jobResult.setResult(tempText);
      jobResultDAO.persist(jobResult);
    } else {
      final JobResult jobResult = results.get(0);
      jobResult.setResult(tempText);
      jobResultDAO.update(jobResult);
    }

    final ParagraphDTO after = fullParagraphDAO.getById(job.getParagraphId());
    EventService.publish(job.getNoteId(), before, after);
  }

  void removeTempOutput(final Job job) {
    jobResultDAO.getByJobId(job.getId()).stream()
            .filter(j -> InterpreterResult.Message.Type.TEXT_TEMP.name().equals(j.getType()))
            .forEach(j -> jobResultDAO.delete(j.getId()));
  }
}

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

import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.core.notebook.JobBatch;
import ru.tinkoff.zeppelin.core.notebook.JobPriority;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.engine.NoteEventService;
import ru.tinkoff.zeppelin.interpreter.thrift.*;
import ru.tinkoff.zeppelin.storage.*;

import javax.annotation.PostConstruct;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class for handle pending jobs
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
@Component
public class InterpreterRequestsHandler extends AbstractHandler {

  private final ApplicationContext applicationContext;
  private static InterpreterRequestsHandler instance;

  public static InterpreterRequestsHandler getInstance() {
    return instance;
  }

  public InterpreterRequestsHandler(final ApplicationContext applicationContext,
                                    final JobBatchDAO jobBatchDAO,
                                    final JobDAO jobDAO,
                                    final JobResultDAO jobResultDAO,
                                    final JobPayloadDAO jobPayloadDAO,
                                    final NoteDAO noteDAO,
                                    final ParagraphDAO paragraphDAO,
                                    final FullParagraphDAO fullParagraphDAO,
                                    final NoteEventService noteEventService) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO, noteEventService);
    this.applicationContext = applicationContext;
  }

  @PostConstruct
  private void init() {
    instance = applicationContext.getBean(InterpreterRequestsHandler.class);
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public RunNoteResult handleRunNote(final String noteUUID,
                                     final String username,
                                     final Set<String> userGroups) {

    final Note note = noteDAO.get(noteUUID);
    final Set<String> userRights = new HashSet<>(userGroups);
    userRights.add(username);

    if (!userRights.retainAll(note.getRunners()) && !isAdmin(userRights)) {
      return new RunNoteResult(RunNoteResultStatus.ERROR, -1, "Insufficient privileges");
    }

    if (noteIsRunning(note)) {
      return new RunNoteResult(RunNoteResultStatus.ERROR, -1, "Already in running state");
    }

    final List<Paragraph> paragraphs = paragraphDAO.getByNoteId(note.getId());
    final long batchId = publishBatch(note, paragraphs, username, userGroups, JobPriority.USER.getIndex());
    if (batchId == -1L) {
      return new RunNoteResult(RunNoteResultStatus.ERROR, -1, "Can't run note (empty note)");
    }

    return new RunNoteResult(RunNoteResultStatus.ACCEPT, batchId, "Note queued");
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public AbortBatchResult handleAbortBatch(final long batchId,
                                          final String username,
                                          final Set<String> userGroups) {

    final JobBatch batch = jobBatchDAO.get(batchId);
    if (batch == null) {
      return new AbortBatchResult(AbortBatchResultStatus.ERROR, "Batch not found");
    }
    final Note note = noteDAO.get(batch.getNoteId());
    final Set<String> userRights = new HashSet<>(userGroups);
    userRights.add(username);

    if (!userRights.retainAll(note.getRunners()) && !isAdmin(userRights)) {
      return new AbortBatchResult(AbortBatchResultStatus.ERROR, "Insufficient privileges");
    }

    abortingBatch(note);

    return new AbortBatchResult(AbortBatchResultStatus.ACCEPT, "OK");
  }


  public BatchStatusResult handleGetBatchStatus(final long batchId,
                                               final String username,
                                               final Set<String> userGroups) {

    final JobBatch batch = jobBatchDAO.get(batchId);
    if (batch == null) {
      return new BatchStatusResult(BatchResultStatus.ERROR, BatchStatus.ERROR, "Batch not found");
    }

    final Note note = noteDAO.get(batch.getNoteId());
    final Set<String> userRights = new HashSet<>(userGroups);
    userRights.add(username);

    if (!userRights.retainAll(note.getRunners()) && !isAdmin(userRights)) {
      return new BatchStatusResult(BatchResultStatus.ERROR, BatchStatus.ERROR, "Insufficient privileges");
    }

    final BatchStatus batchStatus;
    switch (batch.getStatus()) {
      case SAVING:
      case PENDING:
      case RUNNING:
      case ABORTING:
        batchStatus = BatchStatus.RUNNING;
        break;
      case DONE:
        batchStatus = BatchStatus.DONE;
        break;
      case ERROR:
      case ABORTED:
        batchStatus = BatchStatus.ERROR;
        break;
      default:
        batchStatus = BatchStatus.ERROR;
    }
    return new BatchStatusResult(BatchResultStatus.ACCEPT, batchStatus, batch.getStatus().name());
  }

  private boolean isAdmin(final Set<String> userRoles) {
    final Set<String> admin = new HashSet<>();
    admin.addAll(Configuration.getAdminUsers());
    admin.addAll(Configuration.getAdminGroups());
    for (final String availableRole : userRoles) {
      if (admin.contains(availableRole)) {
        return true;
      }
    }
    return false;
  }

}

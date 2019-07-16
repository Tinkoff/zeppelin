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
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration;
import ru.tinkoff.zeppelin.core.notebook.Job;
import ru.tinkoff.zeppelin.core.notebook.JobPriority;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.CredentialService;
import ru.tinkoff.zeppelin.engine.NoteEventService;
import ru.tinkoff.zeppelin.engine.server.AbstractRemoteProcess;
import ru.tinkoff.zeppelin.engine.server.InterpreterRemoteProcess;
import ru.tinkoff.zeppelin.interpreter.Context;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Code;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Message;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Message.Type;
import ru.tinkoff.zeppelin.interpreter.NoteContext;
import ru.tinkoff.zeppelin.interpreter.UserContext;
import ru.tinkoff.zeppelin.interpreter.thrift.PushResult;
import ru.tinkoff.zeppelin.storage.*;

import java.util.List;

/**
 * Class for handle pending jobs
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
@Component
public class PendingHandler extends AbstractHandler {

  private final CredentialService credentialService;

  public PendingHandler(final JobBatchDAO jobBatchDAO,
                        final JobDAO jobDAO,
                        final JobResultDAO jobResultDAO,
                        final JobPayloadDAO jobPayloadDAO,
                        final NoteDAO noteDAO,
                        final ParagraphDAO paragraphDAO,
                        final FullParagraphDAO fullParagraphDAO,
                        final NoteEventService noteEventService,
                        final CredentialService credentialService) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO, noteEventService);
    this.credentialService = credentialService;
  }

  public List<Job> loadJobs() {
    return jobDAO.loadNextPending();
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handle(final Job job,
                     final AbstractRemoteProcess process,
                     final ModuleConfiguration config,
                     final ModuleInnerConfiguration innerConfig) {
    if (!canUseInterpreter(job, config)) {
      final String errorMessage = String.format(
          "User [%s] does not have access to [%s] interpreter.",
          job.getUsername(),
          config.getHumanReadableName()
      );

      final InterpreterResult interpreterResult = new InterpreterResult(
          Code.ABORTED,
          new Message(Type.TEXT, errorMessage)
      );
      setAbortResult(job, jobBatchDAO.get(job.getBatchId()), interpreterResult);
      return;
    }

    // prepare payload
    final String payload = jobPayloadDAO.getByJobId(job.getId()).getPayload();

    // prepare notecontext
    final Note note = noteDAO.get(job.getNoteId());
    final Paragraph paragraph = paragraphDAO.get(job.getParagraphId());

    final Context context = new Context(
        note.getId(),
        note.getUuid(),
        paragraph.getId(),
        paragraph.getUuid(),
        job.getPriority() == JobPriority.SCHEDULER.getIndex()
            ? Context.StartType.SCHEDULED
            : Context.StartType.REGULAR
    );


    context.getNoteContext().put(NoteContext.Z_ENV_NOTE_ID.name(), String.valueOf(job.getNoteId()));
    context.getNoteContext().put(NoteContext.Z_ENV_NOTE_UUID.name(), String.valueOf(note.getUuid()));
    context.getNoteContext().put(NoteContext.Z_ENV_PARAGRAPH_ID.name(), String.valueOf(job.getParagraphId()));
    context.getNoteContext().put(NoteContext.Z_ENV_PARAGRAPH_SHEBANG.name(), job.getShebang());

    context.getUserContext().put(UserContext.Z_ENV_USER_NAME.name(), job.getUsername());
    context.getUserContext().put(UserContext.Z_ENV_USER_ROLES.name(), job.getRoles().toString());

    // put all available credentials
    credentialService.getUserReadableCredentials(job.getUsername(), job.getRoles(), false)
        .forEach(c -> context.getUserContext().put(c.getKey(), c.getValue()));

    // prepare configuration
    innerConfig.getProperties()
        .forEach((p, v) -> context.getConfiguration().put(p, String.valueOf(v.getCurrentValue())));

    final PushResult result = ((InterpreterRemoteProcess) process).push(payload, new Gson().toJson(context));
    if (result == null) {
      return;
    }

    switch (result.getStatus()) {
      case ACCEPT:
        setRunningState(job, result.getInterpreterProcessUUID(), result.getInterpreterJobUUID());
        break;
      case DECLINE:
      case ERROR:
      default:
        // SKIP
    }
  }

  private boolean canUseInterpreter(final Job job, final ModuleConfiguration option) {
    if (!option.getPermissions().isEnabled()) {
      return true;
    }
    final List<String> owners = option.getPermissions().getOwners();
    if (owners.isEmpty()) {
      return true;
    }
    if (owners.contains(job.getUsername())) {
      return true;
    }
    for (final String role : job.getRoles()) {
      if (owners.contains(role)) {
        return true;
      }
    }
    return false;
  }
}

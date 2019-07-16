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

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleSource;
import ru.tinkoff.zeppelin.core.notebook.Job;
import ru.tinkoff.zeppelin.core.notebook.JobBatch;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.engine.NoteEventService;
import ru.tinkoff.zeppelin.engine.server.RemoteProcessStarter;
import ru.tinkoff.zeppelin.engine.server.RemoteProcessType;
import ru.tinkoff.zeppelin.interpreter.PredefinedInterpreterResults;
import ru.tinkoff.zeppelin.storage.*;

/**
 * Class for starting interpreter process
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
@Component
public class InterpreterStarterHandler extends AbstractHandler {

  public InterpreterStarterHandler(final JobBatchDAO jobBatchDAO,
                                   final JobDAO jobDAO,
                                   final JobResultDAO jobResultDAO,
                                   final JobPayloadDAO jobPayloadDAO,
                                   final NoteDAO noteDAO,
                                   final ParagraphDAO paragraphDAO,
                                   final FullParagraphDAO fullParagraphDAO,
                                   final NoteEventService noteEventService) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO, noteEventService);
  }


  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handle(final Job job,
                     final ModuleConfiguration config,
                     final ModuleInnerConfiguration innerConfig,
                     final ModuleSource source,
                     final String thriftAddr,
                     final int thriftPort) {

    final JobBatch batch = jobBatchDAO.get(job.getBatchId());
    if (config == null) {
      setErrorResult(job, batch, PredefinedInterpreterResults.INTERPRETER_NOT_FOUND);
      return;
    }

    if (!config.isEnabled()) {
      setErrorResult(job, batch, PredefinedInterpreterResults.INTERPRETER_DISABLED);
    }

    if (source == null) {
      setErrorResult(job, batch, PredefinedInterpreterResults.INTERPRETER_NOT_FOUND);
      return;
    }

    if (source.getStatus() != ModuleSource.Status.INSTALLED) {
      setErrorResult(job, batch, PredefinedInterpreterResults.INTERPRETER_NOT_FOUND);
      return;
    }

    RemoteProcessStarter.start(
            job.getShebang(),
            RemoteProcessType.INTERPRETER,
            source.getPath(),
            innerConfig.getClassName(),
            thriftAddr,
            thriftPort,
            config.getJvmOptions(),
            config.getConcurrentTasks(),
            config.getRegularTTL(),
            config.getScheduledTTL(),
            Configuration.getInstanceMarkerPrefix());
  }
}

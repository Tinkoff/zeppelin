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

package ru.tinkoff.zeppelin.engine;

import ru.tinkoff.zeppelin.storage.ModuleConfigurationDAO;
import ru.tinkoff.zeppelin.storage.ModuleInnerConfigurationDAO;
import ru.tinkoff.zeppelin.storage.ModuleSourcesDAO;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleSource;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleConfiguration;
import ru.tinkoff.zeppelin.core.notebook.*;
import ru.tinkoff.zeppelin.engine.handler.*;
import ru.tinkoff.zeppelin.engine.server.AbstractRemoteProcess;
import ru.tinkoff.zeppelin.engine.server.RemoteProcessType;
import ru.tinkoff.zeppelin.interpreter.thrift.PingResult;
import ru.tinkoff.zeppelin.interpreter.thrift.PingResultStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Core class of logic
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */

@Lazy(false)
@DependsOn({"configuration", "thriftBootstrap"})
@Component
public class NoteExecutorService {

  private final AbortHandler abortHandler;
  private final PendingHandler pendingHandler;
  private final InterpreterDeadHandler interpreterDeadHandler;
  private final ModuleConfigurationDAO moduleConfigurationDAO;
  private final ModuleInnerConfigurationDAO moduleInnerConfigurationDAO;
  private final ModuleSourcesDAO moduleSourcesDAO;
  private final InterpreterStarterHandler interpreterStarterHandler;
  private final SchedulerHandler schedulerHandler;
  private final ExecutionHandler executionHandler;
  private final ThriftServerBootstrap serverBootstrap;
  private final BaseConfigurationService baseConfigurationService;

  public NoteExecutorService(final AbortHandler abortHandler,
                             final PendingHandler pendingHandler,
                             final InterpreterDeadHandler interpreterDeadHandler,
                             final ModuleConfigurationDAO moduleConfigurationDAO,
                             final ModuleInnerConfigurationDAO moduleInnerConfigurationDAO,
                             final ModuleSourcesDAO moduleSourcesDAO,
                             final InterpreterStarterHandler interpreterStarterHandler,
                             final SchedulerHandler schedulerHandler,
                             final ExecutionHandler executionHandler,
                             final ThriftServerBootstrap serverBootstrap,
                             final BaseConfigurationService baseConfigurationService) {
    this.abortHandler = abortHandler;
    this.pendingHandler = pendingHandler;
    this.interpreterDeadHandler = interpreterDeadHandler;
    this.moduleConfigurationDAO = moduleConfigurationDAO;
    this.moduleInnerConfigurationDAO = moduleInnerConfigurationDAO;
    this.moduleSourcesDAO = moduleSourcesDAO;
    this.interpreterStarterHandler = interpreterStarterHandler;
    this.schedulerHandler = schedulerHandler;
    this.executionHandler = executionHandler;
    this.serverBootstrap = serverBootstrap;
    this.baseConfigurationService = baseConfigurationService;
  }


  @Scheduled(fixedDelay = 1_000)
  private void handlePending() {
    final List<Job> jobs = pendingHandler.loadJobs();
    for (final Job job : jobs) {
      try {
        final ModuleConfiguration config = moduleConfigurationDAO.getByShebang(job.getShebang());
        final ModuleInnerConfiguration innerConfig = config != null
                ? moduleInnerConfigurationDAO.getById(config.getModuleInnerConfigId())
                : null;

        final AbstractRemoteProcess process = job.getShebang() != null
                ? AbstractRemoteProcess.get(job.getShebang(), RemoteProcessType.INTERPRETER)
                : null;

        if (process != null
                && process.getStatus() == AbstractRemoteProcess.Status.READY
                && config != null) {
          pendingHandler.handle(job, process, config, innerConfig);
        } else if (process != null
                && process.getStatus() == AbstractRemoteProcess.Status.STARTING
                && config != null) {
          final String str = ";";
        } else {
          final ModuleSource source = config != null
                  ? moduleSourcesDAO.get(config.getModuleSourceId())
                  : null;

          interpreterStarterHandler.handle(job,
                  config,
                  innerConfig,
                  source,
                  serverBootstrap.getServer().getAddr(),
                  serverBootstrap.getServer().getPort());
        }
      } catch (final Exception e) {
        //log this
      }
    }
  }

  @Scheduled(fixedDelay = 1_000)
  private void handleAbort() {
    final List<JobBatch> batches = abortHandler.loadJobs();
    for (final JobBatch batch : batches) {
      try {
        abortHandler.handle(batch);
      } catch (final Exception e) {
        //log this
      }
    }
  }

  @Scheduled(fixedDelay = 5_000)
  private void actualizeInterpreters() {
    final List<String> liveInterpretersPID = new ArrayList<>();
    final List<String> shebangs = AbstractRemoteProcess.getShebangs(RemoteProcessType.INTERPRETER);
    for (final String shebang : shebangs) {
      final AbstractRemoteProcess process = AbstractRemoteProcess.get(shebang, RemoteProcessType.INTERPRETER);
      if (process.getStatus() == AbstractRemoteProcess.Status.STARTING) {
        continue;
      }
      final ModuleConfiguration config = moduleConfigurationDAO.getByShebang(shebang);
      final PingResult pingResult = process.ping();

      if (pingResult == null
              || pingResult.status == PingResultStatus.KILL_ME
              || config == null
              || !config.isEnabled()) {

        process.forceKill();
      } else {
        liveInterpretersPID.add(process.getUuid());
      }
    }
    interpreterDeadHandler.handle(liveInterpretersPID);
  }

  @Scheduled(fixedDelay = 10_000)
  private void notesScheduler() {

    final boolean isCronEnable = Boolean.logicalAnd(
        Configuration.isCronEnable(),
        baseConfigurationService.get("SCHEDULER_ENABLE_FLAG", true)
    );

    if(isCronEnable) {
      final List<Scheduler> triggered = schedulerHandler.loadJobs();
      for (final Scheduler scheduler : triggered) {
        schedulerHandler.handle(scheduler);
      }
    }
  }

  public void run(final Note note,
                  final List<Paragraph> paragraphs,
                  final String username,
                  final Set<String> roles) {
    executionHandler.run(note, paragraphs, username, roles);
  }

  public void abort(final Note note) {
    abortHandler.abort(note);
  }

}

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
package ru.tinkoff.zeppelin.interpreter.runner;

import ru.tinkoff.zeppelin.interpreter.Context;
import ru.tinkoff.zeppelin.interpreter.Interpreter;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Code;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Message;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Message.Type;
import ru.tinkoff.zeppelin.interpreter.UserContext;
import ru.tinkoff.zeppelin.interpreter.thrift.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


public class RunnerInterpreter extends Interpreter {

  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  private final AtomicBoolean interrupted = new AtomicBoolean(false);


  private volatile long batchId = -1L;
  private volatile String userName;
  private volatile Set<String> userGroups;

  public RunnerInterpreter() {
    super();
  }

  @Override
  public boolean isAlive() {
    return true;
  }

  @Override
  public boolean isOpened() {
    return true;
  }

  @Override
  public void open(final Context context, final String classPath) {
    if (this.configuration == null) {
      this.configuration = new HashMap<>();
    }
    this.configuration.clear();
    this.configuration.putAll(context.getConfiguration());

  }

  @Override
  public boolean isReusableForConfiguration(final Map<String, String> configuration) {
    return this.configuration.equals(configuration);
  }

  @Override
  public void cancel() {
    close();
  }

  @Override
  public void close() {
    interrupted.set(true);

    final ZeppelinThriftService.Client zeppelin = getZeppelin().get();
    if (zeppelin == null) {
      return;
    }

    try {
      zeppelin.handleAbortBatch(batchId, userName, userGroups);
    } catch (final Throwable e) {
      //SKIP
    } finally {
      releaseZeppelin().accept(zeppelin);
    }
  }

  @Override
  public void hibernate() {
    interrupted.set(true);

    final ZeppelinThriftService.Client zeppelin = getZeppelin().get();
    if (zeppelin == null) {
      return;
    }

    try {
      zeppelin.handleAbortBatch(batchId, userName, userGroups);
    } catch (final Throwable e) {
      //SKIP
    } finally {
      releaseZeppelin().accept(zeppelin);
    }
  }

  @Override
  public InterpreterResult interpretV2(final String st,
                                       final Map<String, String> noteContext,
                                       final Map<String, String> userContext,
                                       final Map<String, String> configuration) {
    interrupted.set(false);

    final int maxLife = Integer.parseInt(configuration.get("runner.check.timeout"));
    final int cycleTime = Integer.parseInt(configuration.get("runner.check.cycle.timeout"));

    userName = userContext.get(UserContext.Z_ENV_USER_NAME.name());

    final String groupsTemp = userContext.get(UserContext.Z_ENV_USER_ROLES.name());
    userGroups = new HashSet<>(Arrays.asList(groupsTemp.substring(1, groupsTemp.length() -1).split(", ")));

    ZeppelinThriftService.Client zeppelin;

    RunNoteResult runNoteResult = null;
    zeppelin = getZeppelin().get();
    if (zeppelin != null) {
      try {
        runNoteResult = zeppelin.handleRunNote(st, userName, userGroups);
      } catch (final Throwable e) {
        // SKIP
      } finally {
        releaseZeppelin().accept(zeppelin);
      }
    }

    if (runNoteResult == null) {
      return new InterpreterResult(
              Code.ERROR,
              new InterpreterResult.Message(Type.TEXT, "Error while connecting to zeppelin")
      );
    }

    if (runNoteResult.getStatus() == RunNoteResultStatus.ERROR) {
      return new InterpreterResult(
              Code.ERROR,
              new InterpreterResult.Message(Type.TEXT, runNoteResult.getMessage())
      );
    }

    batchId = runNoteResult.getBatchId();

    final long startTime = java.time.Instant.now().getEpochSecond();
    while (java.time.Instant.now().getEpochSecond() < startTime + maxLife) {

      zeppelin = getZeppelin().get();
      if (zeppelin == null) {
        break;
      }

      // check status
      BatchStatusResult batchStatusResult;
      try {
        batchStatusResult = zeppelin.handleGetBatchStatus(runNoteResult.getBatchId(), userName, userGroups);
      } catch (final Throwable e) {
        batchStatusResult = null;
      } finally {
        releaseZeppelin().accept(zeppelin);
      }

      if (batchStatusResult == null) {
        clearUserData();
        return new InterpreterResult(
                Code.ERROR,
                new InterpreterResult.Message(Type.TEXT, "Error while connecting to zeppelin")
        );
      }

      if (batchStatusResult.getStatus() == BatchResultStatus.ERROR) {
        clearUserData();
        return new InterpreterResult(
                Code.ERROR,
                new InterpreterResult.Message(Type.TEXT, batchStatusResult.message)
        );
      }

      switch (batchStatusResult.getBatchStatus()) {
        case RUNNING:
          final String message = String.format(
                  "Execution in progress. Current status: %s, last check %s",
                  batchStatusResult.message,
                  LocalDateTime.now().format(FORMATTER)
          );
          getTempTextPublisher().accept(message);
          break;
        case DONE:
          clearUserData();
          return new InterpreterResult(
                  Code.SUCCESS,
                  new InterpreterResult.Message(Type.TEXT, "Execution completed successfully")
          );

        case ERROR:
          clearUserData();
          return new InterpreterResult(
                  Code.ERROR,
                  new InterpreterResult.Message(
                          Type.TEXT,
                          String.format("Execution completed with status: %s", batchStatusResult.message)
                  )
          );
      }

      int cycles = 0;
      while (cycles <= cycleTime) {
        try {
          Thread.sleep(1000);
        } catch (final Exception e) {
          // SKIP
        }
        cycles++;
      }
    }

    clearUserData();
    return new InterpreterResult(Code.ERROR, new Message(Type.TEXT, "Killed by user/timeout"));
  }

  private void clearUserData() {
    batchId = -1L;
    userName = null;
    userGroups = null;
  }
}

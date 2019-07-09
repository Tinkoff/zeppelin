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

package ru.tinkoff.zeppelin.remote;

import com.google.gson.Gson;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.thrift.TProcessor;
import ru.tinkoff.zeppelin.interpreter.Interpreter;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.thrift.CancelResult;
import ru.tinkoff.zeppelin.interpreter.thrift.CancelResultStatus;
import ru.tinkoff.zeppelin.interpreter.thrift.PushResult;
import ru.tinkoff.zeppelin.interpreter.thrift.PushResultStatus;
import ru.tinkoff.zeppelin.interpreter.thrift.RemoteInterpreterThriftService;
import ru.tinkoff.zeppelin.interpreter.thrift.ZeppelinThriftService;

public class RemoteInterpreterThread extends AbstractRemoteProcessThread implements RemoteInterpreterThriftService.Iface {

  private final ConcurrentLinkedQueue<Interpreter> cachedInstances = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<Interpreter> workingInstances = new ConcurrentLinkedQueue<>();
  private ExecutorService executor = null;

  private final AtomicInteger shutdownCount = new AtomicInteger(0);

  @Override
  void init(final String zeppelinServerHost,
            final String zeppelinServerPort,
            final String processShebang,
            final String processType,
            final String processClassPath,
            final String processClassName,
            final int poolSize) {

    super.init(zeppelinServerHost,
            zeppelinServerPort,
            processShebang,
            processType,
            processClassPath,
            processClassName,
            poolSize);

    executor = new ThreadPoolExecutor(
            this.poolSize,
            this.poolSize,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1));
  }

  @Override
  protected TProcessor getProcessor() {
    return new RemoteInterpreterThriftService.Processor<>(this);
  }

  @Override
  public PushResult push(final String st,
                         final Map<String, String> noteContext,
                         final Map<String, String> userContext,
                         final Map<String, String> configuration) {
    if (shutdownCount.get() > 0) {
      return new PushResult(PushResultStatus.DECLINE, "", "");
    }

    final Interpreter interpreter;
    try {
      if (!cachedInstances.isEmpty()) {
        final Interpreter polled = cachedInstances.poll();
        if (polled.isReusableForConfiguration(configuration) && polled.isAlive()) {
          interpreter = polled;
        } else {
          polled.close();
          interpreter = (Interpreter) (processClass.newInstance());
        }
      } else {
        interpreter = (Interpreter) processClass.newInstance();
      }

      final UUID uuid = UUID.randomUUID();
      synchronized (interpreter) {
        executor.submit(() -> {
          synchronized (interpreter) {
            interpreter.setSessionUUID(uuid.toString());
            interpreter.setTempTextPublisher(s -> {
              try {
                final ZeppelinThriftService.Client zeppelin = getZeppelin();
                zeppelin.handleInterpreterTempOutput(uuid.toString(), s);
                releaseZeppelin(zeppelin);
              } catch (final Throwable th) {
                //SKIP
              }
            });
            interpreter.setZeppelinSupplier(this::getZeppelin);
            interpreter.setZeppelinConsumer(this::releaseZeppelin);
            workingInstances.offer(interpreter);

            InterpreterResult result;
            try {
              if (!interpreter.isOpened()) {
                interpreter.open(configuration, this.processClasspath);
              }
              result = interpreter.interpretV2(st, noteContext, userContext, configuration);
            } catch (Throwable e) {
              result = new InterpreterResult(
                      InterpreterResult.Code.ERROR,
                      new InterpreterResult.Message(
                              InterpreterResult.Message.Type.TEXT,
                              e.toString()
                      ));
            }
            try {
              final ZeppelinThriftService.Client zeppelin = getZeppelin();
              zeppelin.handleInterpreterResult(interpreter.getSessionUUID(), new Gson().toJson(result));
              releaseZeppelin(zeppelin);
            } catch (final Throwable e) {
              //skip
            }
            workingInstances.remove(interpreter);
            interpreter.setSessionUUID(null);
            cachedInstances.offer(interpreter);
          }
        });
        return new PushResult(PushResultStatus.ACCEPT, uuid.toString(), processUUID.toString());
      }
    } catch (final RejectedExecutionException e) {
      return new PushResult(PushResultStatus.DECLINE, "", "");
    } catch (final Throwable e) {
      return new PushResult(PushResultStatus.ERROR, "", "");
    }
  }

  @Override
  public CancelResult cancel(final String UUID) {
    try {
      for (final Interpreter interpreter : workingInstances) {
        if (interpreter.getSessionUUID().equals(UUID)) {
          interpreter.cancel();
          return new CancelResult(CancelResultStatus.ACCEPT, interpreter.getSessionUUID(), processUUID.toString());
        }
      }
      return new CancelResult(CancelResultStatus.NOT_FOUND, UUID, processUUID.toString());
    } catch (final Throwable e) {
      return new CancelResult(CancelResultStatus.ERROR, UUID, processUUID.toString());
    }
  }

  @Override
  public void shutdown() {
    if (shutdownCount.incrementAndGet() > 5) {
      System.exit(0);
    }

    for (final Interpreter interpreter : workingInstances) {
      try {
        interpreter.cancel();
      } catch (final Throwable e) {
        // log n skip
      }
    }
    super.shutdown();
  }

}

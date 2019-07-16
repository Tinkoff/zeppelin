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
import org.apache.thrift.TProcessor;
import ru.tinkoff.zeppelin.interpreter.Context;
import ru.tinkoff.zeppelin.interpreter.Interpreter;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.thrift.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RemoteInterpreterThread extends AbstractRemoteProcessThread implements RemoteInterpreterThriftService.Iface {


  private class CacheEntity {
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final Interpreter interpreter;
    private volatile long lastRun;

    CacheEntity(final Interpreter interpreter) {
      this.interpreter = interpreter;
      touch();
    }

    AtomicBoolean getIsRunning() {
      return isRunning;
    }

    Interpreter getInterpreter() {
      return interpreter;
    }

    void touch() {
      lastRun = new Date().getTime();
    }

    boolean expired(final long ttlMinutes) {
      return new Date().getTime() > lastRun + ttlMinutes * 60 * 1000;
    }
  }

  private final Map<Long, CacheEntity> regularCache = new ConcurrentHashMap<>();
  private final Map<Long, CacheEntity> scheduledCache = new ConcurrentHashMap<>();

  private final ScheduledExecutorService regularValidator = Executors.newSingleThreadScheduledExecutor();
  private final ScheduledExecutorService scheduledValidator = Executors.newSingleThreadScheduledExecutor();
  private ExecutorService executor = null;

  private final AtomicInteger shutdownCount = new AtomicInteger(0);

  @Override
  protected void init(final String zeppelinServerHost,
                      final String zeppelinServerPort,
                      final String processShebang,
                      final String processType) {

    super.init(
        zeppelinServerHost,
        zeppelinServerPort,
        processShebang,
        processType
    );
  }

  @Override
  protected void postInit() {
    executor = new ThreadPoolExecutor(
        this.poolSize,
        this.poolSize,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(1));

    regularValidator.scheduleWithFixedDelay(
        () -> {
          final Iterator<Map.Entry<Long, CacheEntity>> iterator = regularCache.entrySet().iterator();
          while (iterator.hasNext()) {
            final CacheEntity cacheEntity = iterator.next().getValue();
            if(cacheEntity.expired(regularTTL) && !cacheEntity.isRunning.get()) {
              try {
                cacheEntity.getInterpreter().hibernate();
                iterator.remove();
              } catch (final Throwable th) {
                // SKIP
              }
            }
          }
        },
        60,
        60,
        TimeUnit.SECONDS);

    scheduledValidator.scheduleWithFixedDelay(
        () -> {
          final Iterator<Map.Entry<Long, CacheEntity>> iterator = scheduledCache.entrySet().iterator();
          while (iterator.hasNext()) {
            final CacheEntity cacheEntity = iterator.next().getValue();
            if(cacheEntity.expired(scheduledTTL) && !cacheEntity.isRunning.get()) {
              try {
                cacheEntity.getInterpreter().close();
                iterator.remove();
              } catch (final Throwable th) {
                // SKIP
              }
            }
          }
        },
        60,
        60,
        TimeUnit.SECONDS);
  }

  @Override
  protected TProcessor getProcessor() {
    return new RemoteInterpreterThriftService.Processor<>(this);
  }

  @Override
  public PushResult push(final String st,
                         final String contextJson) {
    if (shutdownCount.get() > 0) {
      return new PushResult(PushResultStatus.DECLINE, "", "");
    }

    final Context context = new Gson().fromJson(contextJson, Context.class);

    final Map<Long, CacheEntity> cache = context.getStartType() == Context.StartType.REGULAR
        ? regularCache
        : scheduledCache;

    final CacheEntity interpreter;
    try {
      if (cache.containsKey(context.getNoteId())) {
        final CacheEntity entity = cache.get(context.getNoteId());
        final Interpreter intp = entity.getInterpreter();

        if (intp.isReusableForConfiguration(context.getConfiguration()) && intp.isAlive()) {
          interpreter = entity;
        } else {
          intp.close();

          final CacheEntity overridedEntity = new CacheEntity((Interpreter) (processClass.newInstance()));
          cache.put(context.getNoteId(), overridedEntity);
          interpreter = overridedEntity;
        }
      } else {
        final CacheEntity overridedEntity = new CacheEntity((Interpreter) (processClass.newInstance()));
        cache.put(context.getNoteId(), overridedEntity);
        interpreter = overridedEntity;
      }

      final UUID uuid = UUID.randomUUID();
      synchronized (interpreter) {
        executor.submit(() -> {
          synchronized (interpreter) {
            interpreter.getInterpreter().setSessionUUID(uuid.toString());
            interpreter.getInterpreter().setTempTextPublisher(s -> {
              try {
                final ZeppelinThriftService.Client zeppelin = getZeppelin();
                zeppelin.handleInterpreterTempOutput(uuid.toString(), s);
                releaseZeppelin(zeppelin);
              } catch (final Throwable th) {
                //SKIP
              }
            });
            interpreter.getInterpreter().setZeppelinSupplier(this::getZeppelin);
            interpreter.getInterpreter().setZeppelinConsumer(this::releaseZeppelin);

            interpreter.getIsRunning().set(true);
            interpreter.touch();

            InterpreterResult result;
            try {
              if (!interpreter.getInterpreter().isOpened()) {
                interpreter.getInterpreter().open(context, this.processClasspath);
              }
              result = interpreter.getInterpreter().interpretV2(
                  st,
                  context.getNoteContext(),
                  context.getUserContext(),
                  context.getConfiguration()
              );
            } catch (final Throwable e) {
              result = new InterpreterResult(
                  InterpreterResult.Code.ERROR,
                  new InterpreterResult.Message(
                      InterpreterResult.Message.Type.TEXT,
                      e.toString()
                  ));
            }
            try {
              final ZeppelinThriftService.Client zeppelin = getZeppelin();
              zeppelin.handleInterpreterResult(interpreter.getInterpreter().getSessionUUID(), new Gson().toJson(result));
              releaseZeppelin(zeppelin);
            } catch (final Throwable e) {
              //skip
            }
            interpreter.getInterpreter().setSessionUUID(null);
            interpreter.getIsRunning().set(false);
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
      final List<CacheEntity> entities = new ArrayList<>();
      entities.addAll(regularCache.values());
      entities.addAll(scheduledCache.values());

      for (final CacheEntity cacheEntity : entities) {
        if (cacheEntity.getIsRunning().get()) {
          if (cacheEntity.getInterpreter().getSessionUUID().equals(UUID)) {
            cacheEntity.getInterpreter().cancel();
            cacheEntity.getIsRunning().set(false);
            return new CancelResult(
                CancelResultStatus.ACCEPT,
                cacheEntity.getInterpreter().getSessionUUID(),
                processUUID.toString()
            );
          }
        }
      }
      return new CancelResult(CancelResultStatus.NOT_FOUND, UUID, processUUID.toString());
    } catch (final Throwable e) {
      return new CancelResult(CancelResultStatus.ERROR, UUID, processUUID.toString());
    }
  }

  @Override
  public FinishResultStatus finish(final String contextJson) {
    final Context context = new Gson().fromJson(contextJson, Context.class);
    try {
      if (context.getStartType() == Context.StartType.SCHEDULED) {
        if (scheduledCache.containsKey(context.getNoteId())) {
          final CacheEntity cacheEntity = scheduledCache.get(context.getNoteId());
          cacheEntity.getInterpreter().close();
          scheduledCache.remove(context.getNoteId());
          return FinishResultStatus.ACCEPT;
        } else {
          return FinishResultStatus.NOT_FOUND;
        }
      } else {
        return FinishResultStatus.ACCEPT;
      }
    } catch (final Throwable th) {
      return FinishResultStatus.ERROR;
    }
  }

  @Override
  public void shutdown() {
    if (shutdownCount.incrementAndGet() > 5) {
      super.shutdown();
    }

    final List<CacheEntity> entities = new ArrayList<>();
    entities.addAll(regularCache.values());
    entities.addAll(scheduledCache.values());

    for (final CacheEntity cacheEntity : entities) {
      if (cacheEntity.getIsRunning().get()) {
        try {
          cacheEntity.getInterpreter().cancel();
          cacheEntity.getIsRunning().set(false);
        } catch (final Throwable e) {
          // log n skip
        }
      }
    }
    super.shutdown();
  }
}

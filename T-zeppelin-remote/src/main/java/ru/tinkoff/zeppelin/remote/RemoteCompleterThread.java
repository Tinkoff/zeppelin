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

import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TProcessor;
import ru.tinkoff.zeppelin.interpreter.Completer;
import ru.tinkoff.zeppelin.interpreter.thrift.RemoteCompleterThriftService;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RemoteCompleterThread extends AbstractRemoteProcessThread implements RemoteCompleterThriftService.Iface {

  private BlockingQueue<Completer> pool = null;
  private AtomicBoolean isLocked = null;
  private volatile int createdObjects = 0;

  @Override
  void init(final String zeppelinServerHost,
            final String zeppelinServerPort,
            final String processShebang,
            final String processType) {

    super.init(zeppelinServerHost,
        zeppelinServerPort,
        processShebang,
        processType
    );
  }

  @Override
  protected void postInit() {
    pool = new ArrayBlockingQueue<>(this.poolSize, true);
    isLocked = new AtomicBoolean(false);
  }

  @Override
  protected TProcessor getProcessor() {
    return new RemoteCompleterThriftService.Processor<>(this);
  }

  @Override
  public String compete(final String st,
                        final int cursorPosition,
                        final Map<String, String> noteContext,
                        final Map<String, String> userContext,
                        final Map<String, String> configuration) {

    Completer completer = null;
    try {
      completer = acquire();

      if (!completer.isOpened()) {
        completer.open(configuration, this.processClasspath);
      }

      if (!completer.isReusableForConfiguration(configuration) || !completer.isAlive()) {
        throw new RuntimeException("Completer EOL");
      }

      final String result = completer.complete(st, cursorPosition, noteContext, userContext, configuration);

      release(completer);

      return result;
    } catch (final Throwable e) {
      if (completer != null) {
        try {
          completer.close();
        } catch (final Exception e1) {
          //skip
        }
      }
      drop();

      return StringUtils.EMPTY;
    }
  }

  private Completer acquire() throws Exception {
    synchronized (this) {
      if (!isLocked.get()) {
        try {
          final Completer completer = (Completer) processClass.newInstance();
          ++createdObjects;
          return completer;

        } finally {
          if (createdObjects >= poolSize) {
            isLocked.set(true);
          }
        }
      }
    }
    return pool.poll(50, TimeUnit.MILLISECONDS);
  }

  private void release(final Completer resource) {
    pool.add(resource);
  }

  private void drop() {
    synchronized (this) {
      if (--createdObjects < poolSize) {
        isLocked.set(false);
      }
    }
  }

}


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

package ru.tinkoff.zeppelin.completer.python;

import jep.Jep;
import jep.JepException;

import java.util.LinkedList;
import java.util.List;

/**
 * This class need because jep an only be used in
 * the thread that created it. And ApacheShift uses a pool of threads,
 * which does not guarantee the creation and execution of Jep in the same thread.
 */
class JepInThread {

  private final JepThread jepThread;

  JepInThread() {
    jepThread = new JepThread();
    jepThread.start();
  }

  <T> void set(final String name, final T value) throws JepException {
    jepThread.addAction(jep -> jep.set(name, value));
  }

  void eval(final String script) throws JepException {
    jepThread.addAction(jep -> jep.eval(script));
  }

  @SuppressWarnings("SameParameterValue")
  Object getValue(final String valueName) throws JepException {
    return jepThread.getValue(valueName);
  }

  void close() {
    try {
      jepThread.addAction(Jep::close);
      jepThread.interrupt();
    } catch (final JepException e) {
      // ignore
    }
  }

  class JepThread extends Thread {
    private Jep jep;
    private final List<JepConsumer> actions;
    private Throwable exception;

    private String requestedValueName;
    private Object requestedValue;

    JepThread() {
      this.actions = new LinkedList<>();
    }

    @Override
    public void run() {
      // create Jep
      try {
        jep = new Jep();
      } catch (final JepException e) {
        exception = e;
      }

      // listen to actions
      while (!actions.isEmpty() || !isInterrupted()) {
        try {
          if (actions.isEmpty()) {
            if (requestedValueName != null) {
              requestedValue = jep.getValue(requestedValueName);
              requestedValueName = null;
            }
          } else {
            final JepConsumer action = actions.remove(0);
            action.accept(jep);
          }
          Thread.sleep(5);
        } catch (final Throwable e) {
          exception = e;
        }
      }

      // close Jep
      try {
        jep.close();
      } catch (final JepException e) {
        exception = e;
      }
    }

    synchronized void addAction(final JepConsumer jepConsumer) throws JepException {
      checkException();
      actions.add(jepConsumer);
    }

    private void checkException() throws JepException {
      if (exception != null) {
        interrupt();
        final Throwable exceptionTemp = exception;
        exception = null;
        throw new JepException("Jep broken. Can't use it", exceptionTemp);
      }
    }

    synchronized Object getValue(final String valueName) throws JepException {
      if (valueName == null) {
        throw new IllegalArgumentException("Variable name can't be null");
      }
      checkException();
      requestedValueName = valueName;

      while (requestedValueName != null) {
        try {
          Thread.sleep(5);
          checkException();
        } catch (final InterruptedException e) {
          // ignore
        }
      }

      final Object value = this.requestedValue;
      this.requestedValue = null;

      return value;
    }
  }
}

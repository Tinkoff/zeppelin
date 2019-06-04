package ru.tinkoff.zeppelin.completer.python;

import jep.Jep;
import jep.JepException;

import java.util.LinkedList;
import java.util.List;

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

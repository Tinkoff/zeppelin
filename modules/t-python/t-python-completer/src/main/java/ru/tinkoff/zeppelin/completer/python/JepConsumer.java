package ru.tinkoff.zeppelin.completer.python;

import jep.Jep;
import jep.JepException;

@FunctionalInterface
public interface JepConsumer {
  void accept(Jep jep) throws JepException;
}

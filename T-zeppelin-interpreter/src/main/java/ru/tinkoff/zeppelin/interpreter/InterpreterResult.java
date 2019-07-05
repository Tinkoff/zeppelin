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
package ru.tinkoff.zeppelin.interpreter;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Interpreter result template.
 */
public class InterpreterResult implements Serializable {

  /**
   *  Type of result after code execution.
   */
  public enum Code {
    SUCCESS,
    ABORTED,
    ERROR
  }


  private Code code;
  private List<Message> msg = new LinkedList<>();

  public InterpreterResult(final Code code) {
    this.code = code;
  }

  public InterpreterResult(final Code code, final List<Message> msgs) {
    this.code = code;
    msg.addAll(msgs);
  }

  public InterpreterResult(final Code code, final Message msgs) {
    this.code = code;
    msg.add(msgs);
  }

  public InterpreterResult add(final Message message) {
    msg.add(message);
    return this;
  }

  public Code code() {
    return code;
  }

  public List<Message> message() {
    return msg;
  }

  public static class Message implements Serializable {

    public enum Type {
      TEXT,
      TEXT_TEMP,
      HTML,
      ANGULAR,
      TABLE,
      IMG,
      SVG,
      NULL,
      NETWORK,
      METATAG
    }

    Type type;
    String data;

    public Message(final Type type, final String data) {
      this.type = type;
      this.data = data;
    }

    public Type getType() {
      return type;
    }

    public String getData() {
      return data;
    }

    public String toString() {
      return "%" + type.name().toLowerCase() + " " + data;
    }
  }
}

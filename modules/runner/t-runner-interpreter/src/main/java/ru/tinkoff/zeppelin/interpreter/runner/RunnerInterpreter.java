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

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import ru.tinkoff.zeppelin.interpreter.Interpreter;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Code;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Message;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Message.Type;

/**
 *
 */
public class RunnerInterpreter extends Interpreter {

  private static class ResponseDTO {
    public String status;
    public String message;
  }

  private final AtomicBoolean interrupted = new AtomicBoolean(false);

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
  public void open(final Map<String, String> configuration, final String classPath) {
    if (this.configuration == null) {
      this.configuration = new HashMap<>();
    }
    this.configuration.clear();
    this.configuration.putAll(configuration);

  }

  @Override
  public boolean isReusableForConfiguration(final Map<String, String> configuration) {
    return this.configuration.equals(configuration);
  }

  @Override
  public void cancel() {
    interrupted.set(true);
  }

  @Override
  public void close() {
    interrupted.set(true);
  }

  private ResponseDTO run(final String zeppelinBaseUrl, final String noteId) throws IOException {
    final HttpURLConnection conn = (HttpURLConnection) new URL(
        String.format("%s/api/executor/notebook/%s", zeppelinBaseUrl, noteId)
    ).openConnection();
    conn.setRequestMethod("GET");

    final StringBuilder sb = new StringBuilder();
    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      while (reader.ready()) {
        sb.append(reader.readLine());
      }
    }

    return new Gson().fromJson(sb.toString(), ResponseDTO.class);
  }

  @Override
  public InterpreterResult interpretV2(final String st,
                                       final Map<String, String> noteContext,
                                       final Map<String, String> userContext,
                                       final Map<String, String> configuration) {
    final String zeppelinBaseUrl = configuration.get("zeppelin.base.url");

    interrupted.set(false);
    if (!interrupted.get()) {
      try {
        final ResponseDTO response = run(zeppelinBaseUrl, st);
        if (response.status != null && response.status.equals("OK")) {
          return new InterpreterResult(Code.SUCCESS, new Message(Type.TEXT, response.message));
        } else {
          return new InterpreterResult(Code.ERROR, new Message(Type.TEXT, response.message));
        }
      } catch (final IOException e) {
        return new InterpreterResult(Code.ERROR, new Message(Type.TEXT, e.getMessage()));
      }
    }

    return new InterpreterResult(Code.ERROR, new Message(Type.TEXT, "Killed by timeout"));
  }
}

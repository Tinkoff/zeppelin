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
package ru.tinkoff.zeppelin.interpreter.python;

import org.apache.commons.lang3.StringUtils;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;

import java.util.Map;

@SuppressWarnings("unused")
public class PythonInterpreter extends AbstractPythonInterpreter {

  public PythonInterpreter() {
    super();
  }

  @Override
  public InterpreterResult interpretV2(String st,
                                       Map<String, String> noteContext,
                                       Map<String, String> userContext,
                                       Map<String, String> configuration) {

    final PythonInterpreterResult result = super.execute(st, noteContext, userContext, configuration);
    final boolean hasContent = result.getInterpreterResult().message().stream()
            .anyMatch(m -> StringUtils.isNotBlank(m.getData()));
    if (!hasContent) {
      final InterpreterResult.Message message = new InterpreterResult.Message(
              InterpreterResult.Message.Type.TEXT,
              "Ooops, empty result. Return code:" + result.getExitCode());
      result.getInterpreterResult().message().add(message);
    }
    return result.getInterpreterResult();
  }
}

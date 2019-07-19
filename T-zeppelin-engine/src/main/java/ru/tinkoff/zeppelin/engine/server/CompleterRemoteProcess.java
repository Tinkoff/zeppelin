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

package ru.tinkoff.zeppelin.engine.server;

import ru.tinkoff.zeppelin.interpreter.RemoteConfiguration;
import ru.tinkoff.zeppelin.interpreter.thrift.RemoteCompleterThriftService;

import java.util.Map;

public class CompleterRemoteProcess extends AbstractRemoteProcess<RemoteCompleterThriftService.Client> {

  CompleterRemoteProcess(
      final String shebang,
      final AbstractRemoteProcess.Status status,
      final String host,
      final int port,
      final RemoteConfiguration remoteConfiguration) {
    super(shebang, status, host, port, remoteConfiguration);
  }

  public String complete(final String payload,
                         final int cursorPosition,
                         final Map<String, String> noteContext,
                         final Map<String, String> userContext,
                         final Map<String, String> configuration) {
    final RemoteCompleterThriftService.Client client = getConnection();
    if (client == null) {
      return null;
    }

    try {
      return client.compete(payload, cursorPosition, noteContext, userContext, configuration);
    } catch (final Throwable throwable) {
      return null;
    } finally {
      releaseConnection(client);
    }
  }
}

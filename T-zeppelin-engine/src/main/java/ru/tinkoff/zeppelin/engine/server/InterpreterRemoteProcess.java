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
import ru.tinkoff.zeppelin.interpreter.thrift.CancelResult;
import ru.tinkoff.zeppelin.interpreter.thrift.FinishResultStatus;
import ru.tinkoff.zeppelin.interpreter.thrift.PushResult;
import ru.tinkoff.zeppelin.interpreter.thrift.RemoteInterpreterThriftService;

public class InterpreterRemoteProcess extends AbstractRemoteProcess<RemoteInterpreterThriftService.Client> {

  InterpreterRemoteProcess(final String shebang,
                           final Status status,
                           final String host,
                           final int port,
                           final RemoteConfiguration remoteConfiguration) {
    super(shebang, status, host, port, remoteConfiguration);
  }

  public PushResult push(final String payload,
                         final String contextJson) {
    final RemoteInterpreterThriftService.Client client = getConnection();
    if(client == null) {
      return null;
    }

    try {
      return client.push(payload, contextJson);
    } catch (final Throwable throwable) {
      return null;
    } finally {
      releaseConnection(client);
    }
  }

  public FinishResultStatus finish(final String contextJson) {
    final RemoteInterpreterThriftService.Client client = getConnection();
    if(client == null) {
      return null;
    }

    try {
      return client.finish(contextJson);
    } catch (final Throwable throwable) {
      return null;
    } finally {
      releaseConnection(client);
    }
  }

  public CancelResult cancel(final String interpreterJobUUID) {
    final RemoteInterpreterThriftService.Client client = getConnection();
    if(client == null) {
      return null;
    }

    try {
      return client.cancel(interpreterJobUUID);
    } catch (final Throwable throwable) {
      return null;
    } finally {
      releaseConnection(client);
    }
  }

}

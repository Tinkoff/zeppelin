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

import com.google.gson.Gson;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.zeppelin.Repository;
import ru.tinkoff.zeppelin.SystemEvent;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.engine.handler.InterpreterRequestsHandler;
import ru.tinkoff.zeppelin.engine.handler.InterpreterResultHandler;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.PredefinedInterpreterResults;
import ru.tinkoff.zeppelin.interpreter.thrift.*;
import ru.tinkoff.zeppelin.storage.SystemEventType.ET;
import ru.tinkoff.zeppelin.storage.ZLog;

import java.util.List;
import java.util.Set;

/**
 * Thrift server for external interpreter process
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
public class RemoteProcessServer {

  private TServerSocket serverSocket;
  private TThreadPoolServer thriftServer;

  private String remoteServerClassPath;

  public void initSources(final List<Repository> repositories) {
    ModuleInstaller.uninstallInterpreter("remote-server");
    ModuleInstaller.install(
            "remote-server",
            "org.apache.zeppelin:T-zeppelin-remote:default",
            repositories
    );
    remoteServerClassPath = ModuleInstaller.getDirectory("remote-server");
  }

  public void start() throws TTransportException {
    this.serverSocket = new TServerSocket(Configuration.getThriftPort());

    final Thread startingThread = new Thread(() -> {
      ZLog.log(ET.REMOTE_PROCESS_SERVER_STARTING,
              String.format("RemoteProcessServer запускается по адресу %s:%s",
                      serverSocket.getServerSocket().getInetAddress().getHostAddress(),
                      serverSocket.getServerSocket().getLocalPort()), SystemEvent.SYSTEM_USERNAME);

      final ZeppelinThriftService.Processor<ZeppelinThriftService.Iface> processor;
      processor = new ZeppelinThriftService.Processor<>(new ZeppelinThriftService.Iface() {

        @Override
        public void registerInterpreterProcess(final RegisterInfo registerInfo) {
          AbstractRemoteProcess.handleRegisterEvent(registerInfo);
        }

        @Override
        public void handleInterpreterResult(final String UUID, final String payload) {
          try {
            InterpreterResultHandler.getInstance().handleResult(UUID, new Gson().fromJson(payload, InterpreterResult.class));
          } catch (final Throwable e) {
            final InterpreterResult error = PredefinedInterpreterResults.ERROR_WHILE_INTERPRET;
            error.add(new InterpreterResult.Message(InterpreterResult.Message.Type.TEXT, e.getMessage()));
            InterpreterResultHandler.getInstance().handleResult(UUID, error);
          }
        }

        @Override
        public void handleInterpreterTempOutput(final String UUID, final String append) {
          try {
            InterpreterResultHandler.getInstance().handleTempOutput(UUID, append);
          } catch (final Throwable e) {
            // SKIP
          }
        }

        @Override
        public RunNoteResult handleRunNote(final String noteUUID, final String userName, final Set<String> userGroups) {
          try {
            return InterpreterRequestsHandler.getInstance().handleRunNote(noteUUID, userName, userGroups);
          } catch (final Throwable e) {
            return new RunNoteResult(RunNoteResultStatus.ERROR, -1L,  e.getCause().toString());
          }
        }

        @Override
        public AbortBatchResult handleAbortBatch(final long batchId, final String userName, final Set<String> userGroups)  {
          try {
            return InterpreterRequestsHandler.getInstance().handleAbortBatch(batchId, userName, userGroups);
          } catch (final Throwable e) {
            return new AbortBatchResult(AbortBatchResultStatus.ERROR, e.getCause().toString());
          }
        }

        @Override
        public BatchStatusResult handleGetBatchStatus(final long batchId, final String userName, final Set<String> userGroups)  {
          try {
            return InterpreterRequestsHandler.getInstance().handleGetBatchStatus(batchId, userName, userGroups);
          } catch (final Throwable e) {
            return new BatchStatusResult(BatchResultStatus.ERROR, BatchStatus.ERROR, e.getCause().toString());
          }
        }
      });

      thriftServer = new TThreadPoolServer(new TThreadPoolServer.Args(serverSocket).processor(processor));
      thriftServer.serve();
    });
    startingThread.start();
    final long start = System.currentTimeMillis();
    while ((System.currentTimeMillis() - start) < 30 * 1000) {
      if (thriftServer != null && thriftServer.isServing()) {
        break;
      }
      try {
        Thread.sleep(500);
      } catch (final InterruptedException e) {

        ZLog.log(ET.REMOTE_PROCESS_SERVER_START_FAILED,
                String.format("Не удалось запустить RemoteProcessServer по адресу %s:%s",
                        serverSocket.getServerSocket().getInetAddress().getHostAddress(),
                        serverSocket.getServerSocket().getLocalPort()),
                String.format("Ошибка при запуске RemoteProcessServer по адресу %s:%s, ошибка:%s",
                        serverSocket.getServerSocket().getInetAddress().getHostAddress(),
                        serverSocket.getServerSocket().getLocalPort(), e.getMessage()),
                SystemEvent.SYSTEM_USERNAME);
      }
    }

    if (thriftServer != null && !thriftServer.isServing()) {
      throw new TTransportException("Fail to start InterpreterEventServer in 30 seconds.");
    }
    ZLog.log(ET.REMOTE_PROCESS_SERVER_STARTED,
            String.format("RemoteProcessServer успешно запущен по адресу %s:%s",
                    serverSocket.getServerSocket().getInetAddress().getHostAddress(),
                    serverSocket.getServerSocket().getLocalPort()),
            SystemEvent.SYSTEM_USERNAME);
  }

  public void stop() {
    if (thriftServer != null) {
      thriftServer.stop();
    }
    ZLog.log(ET.REMOTE_PROCESS_SERVER_STOPPED,
            String.format("RemoteProcessServer по адресу %s:%s успешно остановлен",
                    serverSocket.getServerSocket().getInetAddress().getHostAddress(),
                    serverSocket.getServerSocket().getLocalPort()),
            SystemEvent.SYSTEM_USERNAME
    );
  }

  public String getAddr() {
    return Configuration.getThriftAddress();
  }

  public int getPort() {
    return Configuration.getThriftPort();
  }

  public String getRemoteServerClassPath() {
    return remoteServerClassPath;
  }

  public TServerSocket getServerSocket() {
    return serverSocket;
  }

  public TThreadPoolServer getThriftServer() {
    return thriftServer;
  }
}

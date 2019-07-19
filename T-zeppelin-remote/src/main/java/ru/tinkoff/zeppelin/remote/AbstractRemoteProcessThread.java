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

import com.google.gson.Gson;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import ru.tinkoff.zeppelin.interpreter.RemoteConfiguration;
import ru.tinkoff.zeppelin.interpreter.thrift.*;

import java.util.UUID;

public abstract class AbstractRemoteProcessThread extends Thread implements RemoteProcessThriftService.Iface {

  private String zeppelinServerHost;
  private String zeppelinServerPort;

  private String processShebang;
  private String processType;
  private String processClassName;
  int poolSize;
  int regularTTL;
  int scheduledTTL;
  String processClasspath;

  private TServerSocket serverTransport;
  private TThreadPoolServer server;

  Class processClass;

  static final UUID processUUID = UUID.randomUUID();

  void init(final String zeppelinServerHost,
            final String zeppelinServerPort,
            final String processShebang,
            final String processType) {
    this.zeppelinServerHost = zeppelinServerHost;
    this.zeppelinServerPort = zeppelinServerPort;
    this.processShebang = processShebang;
    this.processType = processType;
  }



  protected abstract void postInit();


  protected abstract TProcessor getProcessor();

  protected ZeppelinThriftService.Client getZeppelin() {
    try {
      final TTransport transport = new TSocket(zeppelinServerHost, Integer.parseInt(zeppelinServerPort));
      transport.open();

      return new ZeppelinThriftService.Client(new TBinaryProtocol(transport));
    } catch (final Throwable e) {
      return null;
    }
  }

  void releaseZeppelin(final ZeppelinThriftService.Client connection) {
    try {
      connection.getOutputProtocol().getTransport().close();
      connection.getInputProtocol().getTransport().close();
    } catch (final Throwable t) {
      // SKIP
    }
  }


  @Override
  public void run() {
    try {
      serverTransport = createTServerSocket();
      server = new TThreadPoolServer(
          new TThreadPoolServer
              .Args(serverTransport)
              .processor(getProcessor())
      );

      final TTransport transport = new TSocket(zeppelinServerHost, Integer.parseInt(zeppelinServerPort));
      transport.open();

      new Thread(new Runnable() {
        boolean interrupted = false;

        @Override
        public void run() {
          while (!interrupted && !server.isServing()) {
            try {
              Thread.sleep(1000);
            } catch (final InterruptedException e) {
              interrupted = true;
            }
          }

          if (!interrupted) {
            final RegisterInfo registerInfo = new RegisterInfo(
                "127.0.0.1",
                serverTransport.getServerSocket().getLocalPort(),
                processShebang,
                processType,
                processUUID.toString()
            );
            try {
              final ZeppelinThriftService.Client zeppelin = getZeppelin();
              final String configurationJson = zeppelin.registerInterpreterProcess(registerInfo);
              final RemoteConfiguration conf = new Gson().fromJson(configurationJson, RemoteConfiguration.class);

              processClasspath = conf.getProcessClasspath();
              processClassName = conf.getProcessClassName();
              poolSize = conf.getPoolSize();
              regularTTL = conf.getRegularTTL();
              scheduledTTL = conf.getScheduledTTL();
              processClass = Class.forName(conf.getProcessClassName());
              postInit();

              releaseZeppelin(zeppelin);
            } catch (final Throwable e) {
              shutdown();
            }
          }
        }
      }).start();

      server.serve();
    } catch (final Exception e) {
      throw new IllegalStateException("", e);
    }
  }

  @Override
  public void shutdown() {
    System.exit(0);
  }

  @Override
  public PingResult ping() {
    return new PingResult(PingResultStatus.OK, processUUID.toString());
  }


  private static TServerSocket createTServerSocket() {
    final int start = 1024;
    final int end = 65535;
    for (int i = start; i <= end; ++i) {
      try {
        return new TServerSocket(i);
      } catch (final Exception e) {
        // ignore this
      }
    }
    throw new IllegalStateException("No available port in the portRange: " + start + ":" + end);
  }

}

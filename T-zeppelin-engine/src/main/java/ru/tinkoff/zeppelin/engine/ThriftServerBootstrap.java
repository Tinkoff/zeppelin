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

package ru.tinkoff.zeppelin.engine;

import ch.qos.logback.classic.Level;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.engine.server.RemoteProcessServer;
import ru.tinkoff.zeppelin.storage.ModuleRepositoryDAO;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;

@Lazy(false)
@DependsOn({"configuration", "ZLog"})
@Component("thriftBootstrap")
class ThriftServerBootstrap {

  private RemoteProcessServer server;

  public ThriftServerBootstrap() { }

  @PostConstruct
  public void init() throws Exception {
    // disable logging
    Logger rootLogger = LoggerFactory.getILoggerFactory().getLogger("org.apache.thrift");
    ((ch.qos.logback.classic.Logger) rootLogger).setLevel(Level.OFF);

    server = new RemoteProcessServer();
    server.start();
  }

  @Scheduled(fixedDelay = 1_000)
  private void checkServer() throws Exception {
    if(server.getThriftServer().isServing()) {
      return;
    }

    try {
      server.getThriftServer().stop();
    } catch (final Exception e) {
      //SKIP
    }

    try {
      server.getServerSocket().close();
    } catch (final Exception e) {
      //SKIP
    }

    server = new RemoteProcessServer();
    server.start();
  }

  @PreDestroy
  private void destroy() {
    server.stop();
  }

  public RemoteProcessServer getServer(){
    return server;
  }
}

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

import org.slf4j.ILoggerFactory;
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

@Lazy(false)
@DependsOn("configuration")
@Component("thriftBootstrap")
class ThriftServerBootstrap {

  private final ModuleRepositoryDAO repositoryDAO;
  private RemoteProcessServer server;

  public ThriftServerBootstrap(final ModuleRepositoryDAO repositoryDAO) {
    this.repositoryDAO = repositoryDAO;
  }

  @PostConstruct
  public void init() throws Exception {

    server = new RemoteProcessServer();
    server.initSources(repositoryDAO.getAll());
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
    server.initSources(repositoryDAO.getAll());
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

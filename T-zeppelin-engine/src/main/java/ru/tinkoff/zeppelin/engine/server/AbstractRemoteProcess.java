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
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import ru.tinkoff.zeppelin.interpreter.RemoteConfiguration;
import ru.tinkoff.zeppelin.interpreter.thrift.PingResult;
import ru.tinkoff.zeppelin.interpreter.thrift.RegisterInfo;
import ru.tinkoff.zeppelin.interpreter.thrift.RemoteProcessThriftService;

import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Represent Interpreter process
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
public abstract class AbstractRemoteProcess<T extends RemoteProcessThriftService.Client> {

  public enum Status {
    STARTING,
    READY
  }

  private static final Map<RemoteProcessType, Map<String, AbstractRemoteProcess>> processMap = new ConcurrentHashMap<>();

  static void starting(final String shebang,
                       final RemoteProcessType processType,
                       final RemoteConfiguration remoteConfiguration) {

    if (!processMap.containsKey(processType)) {
      processMap.put(processType, new ConcurrentHashMap<>());
    }
    final AbstractRemoteProcess process;

    switch (processType) {
      case INTERPRETER:
        process = new InterpreterRemoteProcess(
            shebang,
            AbstractRemoteProcess.Status.STARTING,
            null,
            -1,
            remoteConfiguration);
        break;
      case COMPLETER:
        process = new CompleterRemoteProcess(
            shebang,
            AbstractRemoteProcess.Status.STARTING,
            null,
            -1,
            remoteConfiguration);
        break;
      default:
        throw new IllegalArgumentException();
    }

    processMap.get(processType).put(shebang, process);
  }

  static String handleRegisterEvent(final RegisterInfo registerInfo) {
    final RemoteProcessType processType = RemoteProcessType.valueOf(registerInfo.getProcessType());
    if (!processMap.containsKey(processType)) {
      processMap.put(processType, new ConcurrentHashMap<>());
    }
    if (processMap.get(processType).containsKey(registerInfo.getShebang())) {
      final AbstractRemoteProcess process = processMap.get(processType).get(registerInfo.getShebang());
      process.host = registerInfo.getHost();
      process.port = registerInfo.getPort();
      process.uuid = registerInfo.getProcessUUID();
      process.status = Status.READY;

      return new Gson().toJson(process.getRemoteConfiguration());
    } else {
      return StringUtils.EMPTY;
    }
  }

  static void remove(final String shebang, final RemoteProcessType processType) {
    if (!processMap.containsKey(processType)) {
      processMap.put(processType, new ConcurrentHashMap<>());
    }
    processMap.get(processType).remove(shebang);
  }

  public static AbstractRemoteProcess get(final String shebang, final RemoteProcessType processType) {
    if(!processMap.containsKey(processType)) {
      processMap.put(processType, new ConcurrentHashMap<>());
    }
    return processMap.get(processType).get(shebang);
  }

  public static List<String> getShebangs(final RemoteProcessType processType) {
    if(!processMap.containsKey(processType)) {
      processMap.put(processType, new ConcurrentHashMap<>());
    }
    return new ArrayList<>(processMap.get(processType).keySet());
  }

  private final String shebang;
  private Status status;

  private String host;
  private int port;
  private String uuid;

  private final RemoteConfiguration remoteConfiguration;

  protected AbstractRemoteProcess(final String shebang,
                                  final Status status,
                                  final String host,
                                  final int port,
                                  final RemoteConfiguration remoteConfiguration) {
    this.shebang = shebang;
    this.status = status;
    this.host = host;
    this.port = port;
    this.uuid = null;
    this.remoteConfiguration = remoteConfiguration;
  }

  public String getShebang() {
    return shebang;
  }

  public Status getStatus() {
    return status;
  }

  public RemoteConfiguration getRemoteConfiguration() {
    return remoteConfiguration;
  }

  @SuppressWarnings("unchecked")
  T getConnection() {
    final TSocket transport = new TSocket(host, port);
    try {
      transport.open();
      final TProtocol protocol = new TBinaryProtocol(transport);

      // a little bit of reflection
      final Class<T> clazz = ((Class<T>) ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
      final Constructor<T> constructor = clazz.getConstructor(TProtocol.class);
      return constructor.newInstance(protocol);
      //return new RemoteInterpreterThriftService.Client(protocol);

    } catch (final Throwable e) {
      return null;
    }
  }

  void releaseConnection(final RemoteProcessThriftService.Client connection) {
    try {
      connection.getOutputProtocol().getTransport().close();
      connection.getInputProtocol().getTransport().close();
    } catch (final Throwable t) {
      // SKIP
    }
  }

  public String getUuid() {
    return uuid;
  }

  public PingResult ping() {
    final T client = getConnection();
    if (client == null) {
      return null;
    }

    try {
      return client.ping();
    } catch (final Throwable throwable) {
      return null;
    } finally {
      releaseConnection(client);
    }
  }

  public void forceKill() {
    final RemoteProcessThriftService.Client client = getConnection();
    if (client == null) {
      return;
    }

    try {
      client.shutdown();
    } catch (final Throwable throwable) {
      // SKIP
    } finally {
      releaseConnection(client);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final AbstractRemoteProcess that = (AbstractRemoteProcess) o;
    return port == that.port &&
            shebang.equals(that.shebang) &&
            Objects.equals(host, that.host) &&
            Objects.equals(uuid, that.uuid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(shebang, host, port, uuid);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
            .add("shebang='" + shebang + "'")
            .add("status=" + status)
            .add("host='" + host + "'")
            .add("port=" + port)
            .add("uuid='" + uuid + "'")
            .toString();
  }
}

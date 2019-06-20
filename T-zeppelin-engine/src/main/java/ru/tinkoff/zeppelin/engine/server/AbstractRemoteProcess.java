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

import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import ru.tinkoff.zeppelin.SystemEvent;
import ru.tinkoff.zeppelin.interpreter.thrift.PingResult;
import ru.tinkoff.zeppelin.interpreter.thrift.RegisterInfo;
import ru.tinkoff.zeppelin.interpreter.thrift.RemoteProcessThriftService;
import ru.tinkoff.zeppelin.storage.SystemEventType.ET;
import ru.tinkoff.zeppelin.storage.ZLog;


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

  static void starting(final String shebang, final RemoteProcessType processType) {
    ZLog.log(ET.PROCESS_STARTED,
        String.format("Процесс[shebang:%s, type:%s] запускается", shebang, processType.name()),
        SystemEvent.SYSTEM_USERNAME);

    if (!processMap.containsKey(processType)) {
      processMap.put(processType, new ConcurrentHashMap<>());
    }
    final AbstractRemoteProcess process;

    switch (processType) {
      case INTERPRETER:
        process = new InterpreterRemoteProcess(shebang, AbstractRemoteProcess.Status.STARTING, null, -1);
        break;
      case COMPLETER:
        process = new CompleterRemoteProcess(shebang, AbstractRemoteProcess.Status.STARTING, null, -1);
        break;
      default:
        throw new IllegalArgumentException();
    }

    processMap.get(processType).put(shebang, process);
  }

  static void handleRegisterEvent(final RegisterInfo registerInfo) {
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
      ZLog.log(ET.REMOTE_CONNECTION_REGISTERED,
          String.format("Зарегистрировано подключение к удаленному процессу по адресу %s:%s",
              registerInfo.getHost(), String.valueOf(registerInfo.getPort())),
          String.format("Зарегистрировано подключение к удаленному процессу[shebang=%s, host=%s, port=%s, process uuid=%s]",
              registerInfo.getShebang(), registerInfo.getHost(), String.valueOf(registerInfo.getPort()),
              registerInfo.getProcessUUID()), SystemEvent.SYSTEM_USERNAME);

    }
    ZLog.log(ET.BAD_REMOTE_CONNECTION,
            String.format("Подключение к процессу[shebang:%s] не установлено", registerInfo.getShebang()),
            String.format("Процесс [shebang=%s] не найден: host=%s, port=%s, process uuid=%s",
                registerInfo.getShebang(), registerInfo.getHost(), String.valueOf(registerInfo.getPort()),
                registerInfo.getProcessUUID()), SystemEvent.SYSTEM_USERNAME);
  }

  static void remove(final String shebang, final RemoteProcessType processType) {
    if (!processMap.containsKey(processType)) {
      processMap.put(processType, new ConcurrentHashMap<>());
    }

    final AbstractRemoteProcess removedProcess = processMap.get(processType).remove(shebang);
    if (removedProcess == null) {
      ZLog.log(ET.COMPLETED_PROCESS_NOT_FOUND,
          String.format("Системная ошибка, завершенный процесс[%s] не найден", shebang),
          SystemEvent.SYSTEM_USERNAME);
    } else {
      ZLog.log(ET.PROCESS_COMPLETED,
              String.format("Процесс[shebang=%s, uuid=%s] завершен", removedProcess.getShebang(), removedProcess.uuid),
              String.format("Удаленный процесс завершен: shebang=%s, host=%s, port=%s, process uuid=%s, status=%s",
                      removedProcess.getShebang(), removedProcess.host, removedProcess.port, removedProcess.uuid, removedProcess.status)
      );
    }
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
  String uuid;

  protected AbstractRemoteProcess(final String shebang,
                                  final Status status,
                                  final String host,
                                  final int port) {
    this.shebang = shebang;
    this.status = status;
    this.host = host;
    this.port = port;
    this.uuid = null;
  }

  public String getShebang() {
    return shebang;
  }

  public Status getStatus() {
    return status;
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

    } catch (final Exception e) {
      ZLog.log(ET.CONNECTION_FAILED,
          String.format("Ошибка при открытии соедниение по адресу %s:%s", host, port),
          String.format("Ошибка при открытии TSocket[%s:%s], ошибка: %s", host, port, e.getMessage()),
          SystemEvent.SYSTEM_USERNAME);
      return null;
    }
  }

  void releaseConnection(final RemoteProcessThriftService.Client connection) {
    try {
      connection.getOutputProtocol().getTransport().close();
      connection.getInputProtocol().getTransport().close();
    } catch (final Throwable t) {
      ZLog.log(ET.FAILED_TO_RELEASE_CONNECTION,
          String.format("Ошибка при зкарытии соедниения по адресу %s:%s", host, port),
          String.format("Ошибка при зкарытии соедниения по адресу информация о процессе=%s, ошибка=%s",
              this.toString(), t.getMessage()), SystemEvent.SYSTEM_USERNAME);
    }
  }

  public String getUuid() {
    return uuid;
  }

  public PingResult ping() {
    final T client = getConnection();
    if (client == null) {
      ZLog.log(ET.PING_FAILED_CLIENT_NOT_FOUND,
          String.format("Ping: соединение не установлено %s:%s", this.host, String.valueOf(this.port)),
          String.format("Ping: соединение не установлено: %s", this.toString()), SystemEvent.SYSTEM_USERNAME);
      return null;
    }

    try {
      return client.ping();
    } catch (final Throwable throwable) {

      ZLog.log(ET.PING_FAILED,
          String.format("Ping: соединение разорвано %s:%s", this.host, String.valueOf(this.port)),
          String.format("Ping: соединение разорвано: %s, ошибка: %s", this.toString(), throwable.getMessage()),
          SystemEvent.SYSTEM_USERNAME);
      return null;
    } finally {
      releaseConnection(client);
    }
  }

  public void forceKill() {
    ZLog.log(ET.FORCE_KILL_REQUESTED,
        String.format("Вызвано принудительное завершение процесса по адресу %s:%s", this.host, String.valueOf(this.port)),
        String.format("Вызвано принудительное завершение процесса %s", this.toString()),
        SystemEvent.SYSTEM_USERNAME);

    final RemoteProcessThriftService.Client client = getConnection();
    if (client == null) {
      ZLog.log(ET.FORCE_KILL_FAILED_CLIENT_NOT_FOUND,
          String.format("Ошибка при принудительном завершении процесса %s:%s, соединение не установлено", this.host, String.valueOf(this.port)),
          String.format("Ошибка при принудительном завершении процесса %s, соединение не установлено", this.toString()),
          SystemEvent.SYSTEM_USERNAME);
      return;
    }

    try {
      client.shutdown();
    } catch (final Throwable throwable) {
      ZLog.log(ET.FORCE_KILL_FAILED,
          String.format("Ошибка при принудительном завершении процесса %s:%s", this.host, String.valueOf(this.port)),
          String.format("Ошибка при принудительном завершении процесса %s, ошибка:%s", this.toString(), throwable.getMessage()),
          SystemEvent.SYSTEM_USERNAME);
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

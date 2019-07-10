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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import ru.tinkoff.zeppelin.SystemEvent;
import ru.tinkoff.zeppelin.storage.SystemEventType.ET;
import ru.tinkoff.zeppelin.storage.ZLog;


/**
 * Class for execute cmd
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
public class RemoteProcessStarter {

  public static void start(final String shebang,
                           final RemoteProcessType processType,
                           final String interpreterClassPath,
                           final String interpreterClassName,
                           final String thriftAddr,
                           final long thriftPort,
                           final String jvmOptions,
                           final int concurrentTask,
                           final String zeppelinInstance) {

    final String cmd = String.format("java " +
                    " -DzeppelinInstance=%s" +
                    " %s" +
                    " -cp \"./*\"" +
                    " %s" +
                    " -pt %s" +
                    " -ct %s" +
                    " -h %s" +
                    " -p %s" +
                    " -sb %s" +
                    " -tp %s" +
                    " -cp %s" +
                    " -cn %s ",
            zeppelinInstance,
            jvmOptions,
            processType.getRemoteServerClass().getName(),
            processType.getRemoteThreadClass().getName(),
            concurrentTask,
            thriftAddr,
            thriftPort,
            shebang,
            processType.name(),
            interpreterClassPath,
            interpreterClassName
    );
    ZLog.log(ET.REMOTE_PROCESS_START_REQUESTED,
        String.format("Попытка запустить удаленный процесс по адресу: %s:%s", thriftAddr, thriftPort),
        String.format("Попытка запустить удаленный процесс, cnd: %s", cmd),
        SystemEvent.SYSTEM_USERNAME);

    // start server process
    final CommandLine cmdLine = CommandLine.parse(cmd);

    final DefaultExecutor executor = new DefaultExecutor();
    executor.setWorkingDirectory(new File(interpreterClassPath));

    final ExecuteWatchdog watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
    executor.setWatchdog(watchdog);

    final ExecuteResultHandler handler = new ExecuteResultHandler() {
      @Override
      public void onProcessComplete(final int exitValue) {
        AbstractRemoteProcess.remove(shebang, processType);
        ZLog.log(ET.REMOTE_PROCESS_FINISHED,
            String.format("Удаленный процесс по адресу %s:%S успешно завершен", thriftAddr, thriftPort),
            String.format("Удаленный процесс успешно завершен, cmd: %s", cmd),
            SystemEvent.SYSTEM_USERNAME);
      }

      @Override
      public void onProcessFailed(final ExecuteException e) {
        AbstractRemoteProcess.remove(shebang, processType);
        ZLog.log(ET.REMOTE_PROCESS_FAILED,
            String.format("Критическая ошибка в удаленном процессе по адресу %s:%s", thriftAddr, thriftPort),
            String.format("Критическая ошибка в удаленном процессе, cmd: %s, error: %s", cmd, e.getMessage()),
            SystemEvent.SYSTEM_USERNAME);
      }
    };

    try {
      AbstractRemoteProcess.starting(shebang, processType);
      executor.execute(cmdLine, System.getenv(), handler);
    } catch (final IOException e) {
      AbstractRemoteProcess.remove(shebang, processType);
    }
  }
}

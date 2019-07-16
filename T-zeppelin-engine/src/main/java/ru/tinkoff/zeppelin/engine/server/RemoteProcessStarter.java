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

import org.apache.commons.exec.*;
import ru.tinkoff.zeppelin.interpreter.RemoteConfiguration;

import java.io.File;
import java.io.IOException;


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
                           final int regularTTL,
                           final int scheduledTTL,
                           final String zeppelinInstance) {

    final String cmd = String.format("java " +
                    " -DzeppelinInstance=%s" +
                    " %s" +
                    " -cp \"./*\"" +
                    " %s" +
                    " -pt %s" +
                    " -h %s" +
                    " -p %s" +
                    " -sb %s" +
                    " -tp %s",
            zeppelinInstance,
            jvmOptions,
            processType.getRemoteServerClass().getName(),
            processType.getRemoteThreadClass().getName(),
            thriftAddr,
            thriftPort,
            shebang,
            processType.name()
    );

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
      }

      @Override
      public void onProcessFailed(final ExecuteException e) {
        AbstractRemoteProcess.remove(shebang, processType);
      }
    };

    final RemoteConfiguration remoteConfiguration = new RemoteConfiguration(
        interpreterClassPath,
        interpreterClassName,
        concurrentTask,
        regularTTL,
        scheduledTTL
    );
    try {
      AbstractRemoteProcess.starting(shebang, processType, remoteConfiguration);
      executor.execute(cmdLine, System.getenv(), handler);
    } catch (final IOException e) {
      AbstractRemoteProcess.remove(shebang, processType);
    }
  }
}

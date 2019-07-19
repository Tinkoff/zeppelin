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
package ru.tinkoff.zeppelin.interpreter.jdbc.gp;

import org.postgresql.PGConnection;
import ru.tinkoff.zeppelin.commons.jdbc.AbstractJDBCInterpreter;
import ru.tinkoff.zeppelin.commons.jdbc.analyzer.Analyzer;
import ru.tinkoff.zeppelin.interpreter.Context;
import ru.tinkoff.zeppelin.interpreter.jdbc.gp.analyzer.GPAnalyzer;
import ru.tinkoff.zeppelin.interpreter.jdbc.gp.analyzer.GPResourceGroupActivityDTO;

import javax.annotation.Nonnull;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class GPInterpreter extends AbstractJDBCInterpreter {

  private static GPAnalyzer resourceQueueAnalyzer;
  private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

  /**
   * Installs driver if needed and opens the database remoteConnection.
   *
   * @param context interpreter context.
   * @param classPath     class path.
   */
  @Override
  public void open(@Nonnull final Context context, @Nonnull final String classPath) {
    //TODO: add check on driver
    super.open(context, classPath);
    executorService.scheduleWithFixedDelay(this::publishStats, 2, 30, TimeUnit.SECONDS);
    if (resourceQueueAnalyzer == null) {
      resourceQueueAnalyzer = new GPAnalyzer(context);
      getTempTextPublisher().accept(Analyzer.logMessage("Analyzer successfully created"));
      return;
    }
    getTempTextPublisher().accept(Analyzer.logMessage("Using existing analyzer"));
  }

  private void publishStats() {
    if (this.remoteConnection != null && resourceQueueAnalyzer != null) {
      final GPResourceGroupActivityDTO currentStatus =
          (GPResourceGroupActivityDTO) resourceQueueAnalyzer.getResourceQueueActivity(((PGConnection) this.remoteConnection).getBackendPID());
      if (currentStatus != null) {
        getTempTextPublisher().accept(Analyzer.logMessage(currentStatus.toString()));
      }
    }
  }
}

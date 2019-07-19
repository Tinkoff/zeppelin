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

package ru.tinkoff.zeppelin.commons.jdbc.utils;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.util.Map;

public class JDBCInterpolation {

  public static class InterpolateResponse {
    private final String payload;
    private final int cursorPosition;

    public InterpolateResponse(final String payload, final int cursorPosition) {
      this.payload = payload;
      this.cursorPosition = cursorPosition;
    }

    public String getPayload() {
      return payload;
    }

    public int getCursorPosition() {
      return cursorPosition;
    }
  }


  /**
   * Replaces all environment variables in remoteStatement.
   *
   * @param query, initial remoteStatement, never {@code null}.
   * @param intpContext, interpreter context, never {@code null}.
   * @return new remoteStatement with replaced env variables, never {@code null}.
   */
  @Nonnull
  public static String interpolate(@Nonnull final String query,
                                   @Nonnull final Map<String, String> intpContext) {

    String afterInject = query;
    for (final String var : intpContext.keySet()) {
      afterInject = afterInject.replaceAll(var, intpContext.get(var));
    }
    return afterInject;
  }

  public static InterpolateResponse interpolate(final String query,
                                                int cursorPosition,
                                                final Map<String, String> intpContext) {

    final String marker = "<<<CURSOR_POSITION>>>";
    final String preparedPayload = query.substring(0, cursorPosition) + marker + query.substring(cursorPosition);

    final String afterInject = interpolate(preparedPayload, intpContext);
    final int resultCursorPosition = afterInject.indexOf(marker);
    final String resultPayload = afterInject.replace(marker, StringUtils.EMPTY);

    return new InterpolateResponse(resultPayload, resultCursorPosition);
  }

}

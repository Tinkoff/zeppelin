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
package ru.tinkoff.zeppelin.commons.jdbc;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

public class JDBCUtil {

  private JDBCUtil() {

  }

  /**
   * Checks statement for syntax correctness.
   *
   * @param queryString, query which may consist of multiple statements
   * @return error message if query is incorrect, {@code null} otherwise
   */
  @Nullable
  public static String checkSyntax(@Nonnull final String queryString) {
    try {
      for (final String s : splitStatements(queryString)) {
        if (s.trim().toLowerCase().contains("analyze")) {
          // JSQLParser throws exception on "analyze" keyword.
          continue;
        }
        CCJSqlParserUtil.parse(s);
      }
    } catch (final JSQLParserException e) {
      return e.getCause().getMessage();
    }
    return null;
  }

  /**
   * Splits query by semicolon and filter empty statements.
   *
   * @param query, query which may consist of multiple statements
   * @return statements.
   */
  @Nonnull
  public static List<String> splitStatements(@Nonnull final String query) {
    return Arrays.stream(query.split(";"))
        .filter(s -> !s.trim().isEmpty())
        .collect(Collectors.toList());
  }
}

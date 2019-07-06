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

import com.google.common.collect.Lists;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.zeppelin.interpreter.Interpreter;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Code;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Message;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Message.Type;


/**
 * Common JDBC Interpreter: This interpreter can be used for accessing different SQL databases.
 * <p>
 * Before using interpreter you should configure driver, which will be installed at runtime:
 * <ul>
 * <li>{@code driver.className} - driver class name, e.g. {@code org.postgresql.Driver}</li>
 * <li>{@code driver.artifact} - maven driver artifact, e.g. {@code org.postgresql:postgresql:jar:42.2.5}</li>
 * </ul>
 * <p>
 * Specify connection:
 * <ul>
 * <li>{@code connection.user} - username for database connection</li>
 * <li>{@code connection.url} - database url</li>
 * <li>{@code connection.password} - password</li>
 * </ul>
 * <p>
 * Precode and Postcode rules:
 * <ul>
 * <li>If precode fails -> Error result from precode will be returned as total query result</li>
 * <li>If precode succeed, postcode always will be executed</li>
 * <li>If postcode fails -> error will be logged, and connection will be closed.</li>
 * <li></li>
 * </ul>
 */
public abstract class AbstractJDBCInterpreter extends Interpreter {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJDBCInterpreter.class);

  /**
   * Database connection.
   *
   * @see AbstractJDBCInterpreter#cancel()
   * @see AbstractJDBCInterpreter#close()
   * @see AbstractJDBCInterpreter#open(Map, String)
   * @see AbstractJDBCInterpreter#executeQuery(String, boolean)
   */
  @Nullable
  protected volatile Connection connection = null;

  @Nullable
  protected volatile Statement query = null;

  protected static final String CONNECTION_USER_KEY = "connection.user";
  protected static final String CONNECTION_URL_KEY = "connection.url";
  protected static final String CONNECTION_PASSWORD_KEY = "connection.password";

  protected static final String DRIVER_CLASS_NAME_KEY = "driver.className";
  protected static final String DRIVER_ARTIFACT_KEY = "driver.artifact";
  protected static final String DRIVER_ARTIFACT_DEPENDENCY = "driver.artifact.dependency";
  protected static final String DRIVER_MAVEN_REPO_KEY = "driver.maven.repository.url";

  protected static final String QUERY_TIMEOUT_KEY = "query.timeout";
  protected static final String QUERY_ROWLIMIT_KEY = "query.rowlimit";

  public AbstractJDBCInterpreter() {
    super();
  }

  public Iterator<String> getStatementIterator(@Nonnull final String statements) {
    return Collections.singletonList(statements).iterator();
  }

  /**
   * Checks is connection valid (useable) (may took 30 seconds).
   *
   * @return {@code true} if it is able to execute query using this instance.
   */
  @Override
  public boolean isAlive() {
    try {
      return connection != null && !connection.isValid(5);
    } catch (final Throwable e) {
      return false;
    }
  }

  /**
   * Checks if connection wasn't closed.
   *
   * @return {@code true} if connection wasn't closed.
   */
  @Override
  public boolean isOpened() {
    try {
      return connection != null && !connection.isClosed();
    } catch (final Throwable e) {
      return false;
    }
  }

  /**
   * Installs driver if needed and opens the database connection.
   *
   * @param configuration interpreter configuration.
   * @param classPath     class path.
   */
  @Override
  public void open(@Nonnull final Map<String, String> configuration, @Nonnull final String classPath) {
    if (this.configuration == null) {
      this.configuration = new HashMap<>();
    }
    this.configuration.clear();
    this.configuration.putAll(configuration);
    final String className = configuration.get(DRIVER_CLASS_NAME_KEY);
    final String artifact = configuration.get(DRIVER_ARTIFACT_KEY);
    final String artifactDependencies = configuration.get(DRIVER_ARTIFACT_DEPENDENCY);
    final String user = configuration.get(CONNECTION_USER_KEY);
    final String dbUrl = configuration.get(CONNECTION_URL_KEY);
    final String password = configuration.get(CONNECTION_PASSWORD_KEY);

    if (className != null
            && artifact != null
            && user != null
            && dbUrl != null
            && password != null) {

      final String repositpryURL = configuration.getOrDefault(
              DRIVER_MAVEN_REPO_KEY,
              "http://repo1.maven.org/maven2/"
      );
      final List<String> dependencies = new ArrayList<>();
      if (artifactDependencies != null) {
        dependencies.addAll(Arrays.asList(artifactDependencies.split(";")));
      }
      final String dir = JDBCInstallation.installDriver(artifact, dependencies, repositpryURL);
      if (dir != null && !dir.equals("")) {
        final File driverFolder = new File(dir);
        try {
          final List<URL> urls = Lists.newArrayList();
          for (final File file : driverFolder.listFiles()) {
            final URL url = file.toURI().toURL();

            urls.add(new URL("jar:" + url.toString() + "!/"));
          }

          final URLClassLoader classLoader = URLClassLoader.newInstance(urls.toArray(new URL[0]));
          final Class driverClass = Class.forName(className, true, classLoader);
          final Driver driver = (Driver) driverClass.newInstance();

          final Properties authSettings = new Properties();
          authSettings.put("user", user);
          authSettings.put("password", password);
          connection = driver.connect(dbUrl, authSettings);
        } catch (final Exception e) {
          LOGGER.error("SQL driver configured incorrectly", e);
        }
      }
    }
  }

  @Override
  public boolean isReusableForConfiguration(@Nonnull final Map<String, String> configuration) {
    return this.configuration.equals(configuration);
  }

  /**
   * May be called from another thread.
   **/
  @Override
  public void cancel() {
    try {
      query.cancel();
    } catch (final Throwable e) {
      LOGGER.error("Failed to cancel", e);
    }
  }

  /**
   * May be called from another thread.
   */
  @Override
  public void close() {
    if (isOpened()) {
      try {
        connection.abort(Runnable::run);
      } catch (final Throwable e) {
        LOGGER.error("Failed to close", e);
      }
    }
  }

  /**
   * Interprets query.
   * <p>
   * Notice that interpreter should be alive before calling interpreter. {@link AbstractJDBCInterpreter#isAlive()}
   * <p>
   * TODO(egorklimov): check execution logic on cancel!
   * If interpreter would be canceled on precode, {@code precodeResult.code()} would be {@code Code.ERROR}
   * therefore whole interpret process will be finished.
   * If interpreter would be canceled on user query {@code queryResult.code()} would be {@code Code.ERROR},
   * but it would be returned only after postcode execution.
   * If interpreter would be canceled on postcode, {@code postcodeResult.code()} would be {@code Code.ERROR}
   * therefore connection will be closed and ...??
   *
   * @param st            statements to run.
   * @param noteContext   Note context
   * @param userContext   User context
   * @param configuration Interpreter properties
   * @return Interpreter result
   */
  @Nonnull
  @Override
  public InterpreterResult interpretV2(@Nonnull final String st,
                                       @Nonnull final Map<String, String> noteContext,
                                       @Nonnull final Map<String, String> userContext,
                                       @Nonnull final Map<String, String> configuration) {
    final Map<String, String> params = new HashMap<>();
    params.putAll(noteContext);
    params.putAll(userContext);

    final String precode = configuration.get("query.precode");
    if (precode != null && !precode.trim().equals("")) {
      final InterpreterResult precodeResult = executeQuery(
              JDBCInterpolation.interpolate(precode, params),
              false
      );
      if (precodeResult.code().equals(Code.ERROR)) {
        return precodeResult;
      }
    }

    final Iterator<String> statementIterator = getStatementIterator(JDBCInterpolation.interpolate(st, params));
    InterpreterResult queryResult = new InterpreterResult(Code.SUCCESS);
    while (statementIterator.hasNext()) {
      final InterpreterResult subQueryResult = executeQuery(
          statementIterator.next(),
          true
      );
      queryResult.message().addAll(subQueryResult.message());
      if (subQueryResult.code().equals(Code.ERROR)) {
        queryResult = subQueryResult;
        break;
      }
    }

    final String postcode = configuration.get("query.postcode");
    if (postcode != null && !postcode.trim().equals("")) {
      final InterpreterResult postcodeResult = executeQuery(
              JDBCInterpolation.interpolate(postcode, params),
              false
      );
      if (postcodeResult.code().equals(Code.ERROR)) {
        LOGGER.error("Postcode query failed: {}", postcodeResult.message());
        close();
      }
    }
    return queryResult;
  }

  /**
   * Util method to execute a single query.
   *
   * @param queryString   - Query to execute, may consist of multiple statements, never {@code null}.
   * @param processResult - Flag of result processing, if {@code true} - result
   *                      will be converted to table format, otherwise result has no message.
   * @return Result of query execution, never {@code null}.
   */
  @Nonnull
  private InterpreterResult executeQuery(@Nonnull final String queryString, final boolean processResult) {
    // ZP-142 WRONG
    //final String errorMessage = JDBCUtil.checkSyntax(queryString);
    //if (errorMessage != null) {
    //  return new InterpreterResult(Code.ERROR, new Message(Type.TEXT, errorMessage));
    //}

    ResultSet resultSet = null;
    final StringBuilder exception = new StringBuilder();
    try {
      this.query = Objects.requireNonNull(connection).createStatement();
      prepareQuery();

      final InterpreterResult queryResult = new InterpreterResult(Code.SUCCESS);
      // queryString may consist of multiple statements, so it's needed to process all results.
      boolean results = Objects.requireNonNull(this.query).execute(queryString);
      int updateCount = 0;
      do {
        if (results) {
          // if result is ResultSet.
          resultSet = Objects.requireNonNull(this.query).getResultSet();
          if (resultSet != null && processResult) {
            // if it is needed to process result to table format.
            final String processedTable = getResults(resultSet);
            if (processedTable == null) {
              queryResult.add(new Message(Type.TEXT, "Failed to process query result"));
            }
            queryResult.add(new Message(Type.TABLE, processedTable));
          }
        } else {
          // if result is empty or if it is update statement, e.g. insert.
          updateCount = Objects.requireNonNull(this.query).getUpdateCount();
          if (updateCount != -1) {
            queryResult.add(new Message(Type.TEXT,
                    "Query executed successfully. Affected rows: " + updateCount));
          }
        }
        // go to the next result set, previous would be closed.
        results = Objects.requireNonNull(this.query).getMoreResults();
      } while (results || updateCount != -1);
      return queryResult;
    } catch (final Exception e) {
      // increment exception message if smth went wrong.
      exception.append("Exception during processing the query:\n")
              .append(ExceptionUtils.getStackTrace(e))
              .append("\n");
    } finally {
      // cleanup.
      try {
        if (resultSet != null) {
          resultSet.close();
        }
        if (this.query != null) {
          Objects.requireNonNull(this.query).close();
        }
      } catch (final Exception e) {
        // increment exception message if smth went wrong.
        exception.append("Exception during connection closing:\n")
                .append(ExceptionUtils.getStackTrace(e));
      }
    }
    // reachable if smth went wrong during query processing.
    return new InterpreterResult(Code.ERROR, Collections.singletonList(
            new Message(Type.TEXT, exception.toString())));
  }

  /**
   * Sets query timeout and max row count.
   */
  private void prepareQuery() {
    int maxRows = Integer.parseInt(configuration.getOrDefault(QUERY_ROWLIMIT_KEY, "0"));
    if (maxRows < 0) {
      maxRows = 0;
    }
    int timeout = Integer.parseInt(configuration.getOrDefault(QUERY_TIMEOUT_KEY, "0"));
    if (timeout < 0) {
      timeout = 0;
    }
    try {
      Objects.requireNonNull(this.query).setMaxRows(maxRows);
      Objects.requireNonNull(this.query).setQueryTimeout(timeout);
    } catch (final Exception e) {
      LOGGER.error("Failed to set query limits", e);
    }
  }


  /**
   * Converts result set to table.
   *
   * @param resultSet - result set to convert.
   * @return converted result, {@code null} if process failed.
   */
  @Nullable
  private String getResults(final ResultSet resultSet) {
    try {
      final ResultSetMetaData md = resultSet.getMetaData();
      final StringBuilder msg = new StringBuilder();

      for (int i = 1; i < md.getColumnCount() + 1; i++) {
        if (i > 1) {
          msg.append('\t');
        }
        if (md.getColumnLabel(i) != null && !md.getColumnLabel(i).equals("")) {
          msg.append(replaceReservedChars(md.getColumnLabel(i)));
        } else {
          msg.append(replaceReservedChars(md.getColumnName(i)));
        }
      }
      msg.append('\n');

      while (resultSet.next()) {
        for (int i = 1; i < md.getColumnCount() + 1; i++) {
          final Object resultObject;
          final String resultValue;
          resultObject = resultSet.getObject(i);
          if (resultObject == null) {
            resultValue = "null";
          } else {
            resultValue = resultSet.getString(i);
          }
          msg.append(replaceReservedChars(resultValue));
          if (i != md.getColumnCount()) {
            msg.append('\t');
          }
        }
        msg.append('\n');
      }
      return msg.toString();
    } catch (final Exception e) {
      LOGGER.error("Failed to parse result", e);
      return null;
    }
  }

  /**
   * For table response replace Tab and Newline characters from the content.
   */
  private String replaceReservedChars(final String str) {
    if (str == null) {
      return "";
    }
    return str.replace('\t', ' ')
            .replace('\n', ' ');
  }
}

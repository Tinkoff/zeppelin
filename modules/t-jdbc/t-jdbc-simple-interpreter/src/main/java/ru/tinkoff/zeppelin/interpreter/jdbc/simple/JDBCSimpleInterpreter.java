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
package ru.tinkoff.zeppelin.interpreter.jdbc.simple;

import com.google.common.collect.Lists;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.zeppelin.commons.jdbc.H2Manager;
import ru.tinkoff.zeppelin.commons.jdbc.JDBCInstallation;
import ru.tinkoff.zeppelin.commons.jdbc.JDBCInterpolation;
import ru.tinkoff.zeppelin.commons.jdbc.JDBCUtil;
import ru.tinkoff.zeppelin.interpreter.Interpreter;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Code;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Message;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Message.Type;
import ru.tinkoff.zeppelin.interpreter.NoteContext;


/**
 * Common JDBC Interpreter: This interpreter can be used for accessing different SQL databases.
 * <p>
 * Before using interpreter you should configure driver, which will be installed at runtime:
 * <ul>
 * <li>{@code driver.className} - driver class name, e.g. {@code org.postgresql.Driver}</li>
 * <li>{@code driver.artifact} - maven driver artifact, e.g. {@code org.postgresql:postgresql:jar:42.2.5}</li>
 * </ul>
 * <p>
 * Specify remoteConnection:
 * <ul>
 * <li>{@code remoteConnection.user} - username for database remoteConnection</li>
 * <li>{@code remoteConnection.url} - database url</li>
 * <li>{@code remoteConnection.password} - password</li>
 * </ul>
 * <p>
 * Precode and Postcode rules:
 * <ul>
 * <li>If precode fails -> Error result from precode will be returned as total remoteStatement result</li>
 * <li>If precode succeed, postcode always will be executed</li>
 * <li>If postcode fails -> error will be logged, and remoteConnection will be closed.</li>
 * <li></li>
 * </ul>
 */
public class JDBCSimpleInterpreter extends Interpreter {

  private static final Logger LOGGER = LoggerFactory.getLogger(JDBCSimpleInterpreter.class);

  private final H2Manager h2Manager = new H2Manager();

  private final Pattern TBL_NAME_PATTERN = Pattern.compile("(?=(--#localh2.[_a-zA-Z0-9]))");
  private final Pattern LOCAL_TBL_NAME_PATTERN = Pattern.compile("(?=(localh2.[_a-zA-Z0-9]))");
  private final Pattern LINE_COMMENTS_PATTERN = Pattern.compile("(?=(--.+\n))");
  private final Pattern BLOCK_COMMENTS_PATTERN = Pattern.compile("(?=(\\/\\*.+\\*\\/))");
  private final Pattern CREATE_TABLE_PATTERN = Pattern.compile("(?=(.+create.*table\\W*(([a-zA-Z0-1]+\\.)?[a-zA-Z0-1]*)))");
  private final Pattern DELETE_TABLE_PATTERN = Pattern.compile("(?=(.*drop.*table\\W*(([a-zA-Z0-1]+\\.)?[a-zA-Z0-1]*)))");

  /**
   * Database remoteConnection.
   *
   * @see JDBCSimpleInterpreter#cancel()
   * @see JDBCSimpleInterpreter#close()
   * @see JDBCSimpleInterpreter#open(Map, String)
   */
  private volatile Connection remoteConnection = null;

  private volatile Statement remoteStatement = null;

  private static final String CONNECTION_USER_KEY = "remoteConnection.user";
  private static final String CONNECTION_URL_KEY = "remoteConnection.url";
  private static final String CONNECTION_PASSWORD_KEY = "remoteConnection.password";

  private static final String DRIVER_CLASS_NAME_KEY = "driver.className";
  private static final String DRIVER_ARTIFACT_KEY = "driver.artifact";
  private static final String DRIVER_ARTIFACT_DEPENDENCY = "driver.artifact.dependency";
  private static final String DRIVER_MAVEN_REPO_KEY = "driver.maven.repository.url";

  private static final String QUERY_TIMEOUT_KEY = "remoteStatement.timeout";
  private static final String QUERY_ROWLIMIT_KEY = "remoteStatement.rowlimit";
  private static final String QUERY_MAX_SAVE_ROW_LIMIT_KEY = "remoteStatement.maxsaverowlimit";

  public JDBCSimpleInterpreter() {
    super();
  }

  /**
   * Checks is remoteConnection valid (useable) (may took 30 seconds).
   *
   * @return {@code true} if it is able to execute remoteStatement using this instance.
   */
  @Override
  public boolean isAlive() {
    try {
      return remoteConnection != null && !remoteConnection.isValid(5);
    } catch (final Throwable e) {
      return false;
    }
  }

  /**
   * Checks if remoteConnection wasn't closed.
   *
   * @return {@code true} if remoteConnection wasn't closed.
   */
  @Override
  public boolean isOpened() {
    try {
      return remoteConnection != null && !remoteConnection.isClosed();
    } catch (final Throwable e) {
      return false;
    }
  }

  /**
   * Installs driver if needed and opens the database remoteConnection.
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
          remoteConnection = driver.connect(dbUrl, authSettings);
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
      remoteStatement.cancel();
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
        remoteConnection.abort(Runnable::run);
      } catch (final Throwable e) {
        LOGGER.error("Failed to close", e);
      }
    }
  }

  /**
   * Interprets remoteStatement.
   * <p>
   * Notice that interpreter should be alive before calling interpreter. {@link JDBCSimpleInterpreter#isAlive()}
   * <p>
   * TODO(egorklimov): check execution logic on cancel!
   * If interpreter would be canceled on precode, {@code precodeResult.code()} would be {@code Code.ERROR}
   * therefore whole interpret process will be finished.
   * If interpreter would be canceled on user remoteStatement {@code queryResult.code()} would be {@code Code.ERROR},
   * but it would be returned only after postcode execution.
   * If interpreter would be canceled on postcode, {@code postcodeResult.code()} would be {@code Code.ERROR}
   * therefore remoteConnection will be closed and ...??
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


    final String noteContextPath =
            noteContext.get(NoteContext.Z_ENV_NOTES_STORE_PATH.name())
                    + File.separator
                    + noteContext.get(NoteContext.Z_ENV_NOTE_UUID.name())
                    + File.separator
                    + "outputDB";

    //get row_limit configuration parameter to insert in h2 database
    final int rowLimit = Integer.parseInt(configuration.getOrDefault(QUERY_MAX_SAVE_ROW_LIMIT_KEY, "0"));


    final String precode = configuration.get("remoteStatement.precode");
    if(StringUtils.isNotBlank(precode)) {
      try {
        execQuery(JDBCInterpolation.interpolate(precode, params));
      } catch (final Exception e) {
        return new InterpreterResult(Code.ERROR, new Message(Type.TEXT, e.getMessage()));
      }
    }

    Code resultCode = Code.SUCCESS;
    final List<Message> resultMessages = new LinkedList<>();

    final String connectionUrl = "jdbc:h2:file:" + Paths.get(noteContextPath).normalize().toFile().getAbsolutePath();
    try(final Connection h2conn = DriverManager.getConnection(connectionUrl, "sa", "")) {

      this.remoteStatement = remoteConnection.createStatement();
      this.remoteStatement.setMaxRows(Integer.parseInt(configuration.getOrDefault(QUERY_ROWLIMIT_KEY, "0")));
      this.remoteStatement.setQueryTimeout(Integer.parseInt(configuration.getOrDefault(QUERY_TIMEOUT_KEY, "0")));

      final List<String> queries = JDBCUtil.splitStatements(JDBCInterpolation.interpolate(st, params));
        for (String query : queries) {

          final String tblName = TBL_NAME_PATTERN.matcher(query).matches()
                  ? TBL_NAME_PATTERN.matcher(query).group(1).replace("--#localh2.", "")
                  : null;
          query = TBL_NAME_PATTERN.matcher(query).replaceAll("--");

          //delete comments
          String statementCopy = query.toLowerCase();
          statementCopy = LINE_COMMENTS_PATTERN.matcher(statementCopy).replaceAll(StringUtils.EMPTY);
          statementCopy = BLOCK_COMMENTS_PATTERN.matcher(statementCopy).replaceAll(StringUtils.EMPTY);

          final String createTableName = CREATE_TABLE_PATTERN.matcher(statementCopy).matches()
                  ? CREATE_TABLE_PATTERN.matcher(statementCopy).group(1)
                  : null;

          final String deleteTableName = DELETE_TABLE_PATTERN.matcher(statementCopy).matches()
                  ? DELETE_TABLE_PATTERN.matcher(statementCopy).group(1)
                  : null;

          // RESTORE TABLES
          final Matcher usedTablesMather = LOCAL_TBL_NAME_PATTERN.matcher(query);
          while (usedTablesMather.find()) {

            final String match = usedTablesMather.group(1);
            final String tableName = match.replace("localh2.", "");

            final Statement statement = h2conn.createStatement();
            statement.execute(String.format("SELECT * FROM %s.%s;", H2Manager.Type.REAL.getSchemaName(), tableName));
            // TODO: FIX SCHEMA NAME
            final String info = h2Manager.saveTable(
                    "PUBLIC",
                    tableName,
                    statement.getResultSet(),
                    -1L,
                    rowLimit,
                    remoteConnection
            );
            resultMessages.add(new Message(Type.TEXT, info));

            query = query.replace(match, tableName);
          }

          this.remoteStatement.execute(query);

          if (this.remoteStatement.getResultSet() != null) {
            final String info = h2Manager.saveTable(H2Manager.Type.REAL.getSchemaName(),
                    tblName,
                    this.remoteStatement.getResultSet(),
                    -1L,
                    rowLimit,
                    h2conn
            );
            resultMessages.add(new Message(Type.TEXT, info));

          } else if (this.remoteStatement.getUpdateCount() != -1 && createTableName != null) {
            final String info = h2Manager.saveTable(
                    H2Manager.Type.VIRTUAL.getSchemaName(),
                    createTableName.split(".")[createTableName.split(".").length - 1],
                    remoteConnection.createStatement()
                            .executeQuery("SELECT * FROM " + createTableName + " LIMIT 100;"),
                    this.remoteStatement.getUpdateCount(),
                    100,
                    h2conn);
            resultMessages.add(new Message(Type.TEXT, info));

            resultMessages.add(new Message(Type.TEXT,
                    "Query executed successfully. Affected rows: " + this.remoteStatement.getUpdateCount()));

          } else if (this.remoteStatement.getUpdateCount() != -1 && deleteTableName != null) {

          } else {
            resultMessages.add(new Message(Type.TEXT,
                    "Query executed successfully. Affected rows: " + this.remoteStatement.getUpdateCount()));
          }
        }

    } catch (final Throwable th) {
      resultMessages.add(new Message(Type.TEXT, ExceptionUtils.getStackTrace(th)));
      resultCode = Code.ERROR;
    }

    final String postCode = configuration.get("remoteStatement.postcode");
    if(StringUtils.isNotBlank(postCode)) {
      try {
        execQuery(JDBCInterpolation.interpolate(postCode, params));
      } catch (final Throwable th) {
        resultMessages.add(new Message(Type.TEXT, ExceptionUtils.getStackTrace(th)));
        resultCode = Code.ERROR;
      }
    }
    return new InterpreterResult(resultCode, resultMessages);
  }


  private void execQuery(final String query) throws Exception {
    try {
      this.remoteStatement = remoteConnection.createStatement();
      this.remoteStatement.execute(query);
    } catch (final Throwable th) {
        throw new Exception(ExceptionUtils.getStackTrace(th));
    } finally {
      try {
        if (this.remoteStatement != null) {
          this.remoteStatement.close();
        }
      } catch (final Throwable e) {
        // SKIP
      }
    }
  }

}

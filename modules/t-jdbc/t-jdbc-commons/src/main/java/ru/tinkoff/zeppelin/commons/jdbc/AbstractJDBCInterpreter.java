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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.zeppelin.commons.jdbc.utils.JDBCInstallation;
import ru.tinkoff.zeppelin.commons.jdbc.utils.JDBCInterpolation;
import ru.tinkoff.zeppelin.interpreter.Context;
import ru.tinkoff.zeppelin.interpreter.Interpreter;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Code;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Message;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Message.Type;
import ru.tinkoff.zeppelin.interpreter.NoteContext;
import ru.tinkoff.zeppelin.interpreter.content.H2Manager;
import ru.tinkoff.zeppelin.interpreter.content.H2Table;
import ru.tinkoff.zeppelin.interpreter.content.H2TableConverter;
import ru.tinkoff.zeppelin.interpreter.content.H2TableType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


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
public abstract class AbstractJDBCInterpreter extends Interpreter {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJDBCInterpreter.class);

  /**
   * Database remoteConnection.
   *
   * @see AbstractJDBCInterpreter#cancel()
   * @see AbstractJDBCInterpreter#close()
   * @see AbstractJDBCInterpreter#open(Context, String)
   * //* @see AbstractJDBCInterpreter#executeQuery(String, boolean)
   */
  @Nullable
  protected volatile Connection remoteConnection = null;

  @Nullable
  private volatile Statement remoteStatement = null;


  private final H2Manager h2Manager = new H2Manager();
  private final H2TableConverter h2TableConverter = new H2TableConverter();

  private static final String QUERY_MAX_SAVE_ROW_LIMIT_KEY = "query.maxsaverowlimit";

  private static final String CONNECTION_USER_KEY = "connection.user";
  private static final String CONNECTION_URL_KEY = "connection.url";
  private static final String CONNECTION_PASSWORD_KEY = "connection.password";

  private static final String DRIVER_CLASS_NAME_KEY = "driver.className";
  private static final String DRIVER_ARTIFACT_KEY = "driver.artifact";
  private static final String DRIVER_ARTIFACT_DEPENDENCY = "driver.artifact.dependency";
  private static final String DRIVER_MAVEN_REPO_KEY = "driver.maven.repository.url";

  private static final String QUERY_TIMEOUT_KEY = "query.timeout";
  private static final String QUERY_ROWLIMIT_KEY = "query.rowlimit";
  private final Pattern TBL_NAME_PATTERN = Pattern.compile("(?=(--#localh2\\.([_a-zA-Z0-9])+))");
  private final Pattern LOCAL_TBL_NAME_PATTERN = Pattern.compile("(?=(local\\.([_a-zA-Z0-9])+))");
  private final Pattern LINE_COMMENTS_PATTERN = Pattern.compile("--.+\n");
  private final Pattern BLOCK_COMMENTS_PATTERN = Pattern.compile("\\/\\*.+\\*\\/");
  private final Pattern CREATE_TABLE_PATTERN = Pattern.compile(
          "(?=(.*create( +|\\W)+table( +|\\W+)+" +
                  "(if( +|\\W+)+not( +|\\W+)+exists( +|\\W+)+)?" +
                  "(([a-zA-Z0-9]+\\.)?([a-zA-Z0-9]*))))"//table name = group 10
  );
  private final Pattern DELETE_TABLE_PATTERN = Pattern.compile(
          "(?=(.*drop( +|\\W)+table( +|\\W+)+" +
                  "(if( +|\\W+)+exists( +|\\W+)+)?" +
                  "(([a-zA-Z0-9]+\\.)?([a-zA-Z0-9]*))))"//table name = group 9
  );

  public AbstractJDBCInterpreter() {
    super();
  }

  public Iterator<String> getStatementIterator(@Nonnull final String statements) {
    return Collections.singletonList(statements).iterator();
  }

  /**
   * Checks is remoteConnection valid (useable) (may took 5 seconds).
   *
   * @return {@code true} if it is able to execute remoteStatement using this instance.
   */
  @Override
  public boolean isAlive() {
    try {
      return remoteConnection != null && remoteConnection.isValid(5);
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
   * @param context   interpreter context.
   * @param classPath class path.
   */
  @Override
  public void open(@Nonnull final Context context, @Nonnull final String classPath) {
    if (this.configuration == null) {
      this.configuration = new HashMap<>();
    }
    this.configuration.clear();
    this.configuration.putAll(context.getConfiguration());
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

      final String repositoryURL = configuration.getOrDefault(
              DRIVER_MAVEN_REPO_KEY,
              "http://repo1.maven.org/maven2/"
      );
      final List<String> dependencies = new ArrayList<>();
      if (artifactDependencies != null) {
        dependencies.addAll(Arrays.asList(artifactDependencies.split(";")));
      }
      final String dir = JDBCInstallation.installDriver(artifact, dependencies, repositoryURL);
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
        remoteStatement.cancel();
      } catch (final Throwable e) {
        LOGGER.error("Failed to cancel", e);
      }
      try {
        remoteConnection.abort(Runnable::run);
        remoteConnection = null;
      } catch (final Throwable e) {
        LOGGER.error("Failed to close", e);
      }
    }
  }

  /**
   * May be called from another thread.
   */
  @Override
  public void hibernate() {
    if (isOpened()) {
      try {
        remoteStatement.cancel();
      } catch (final Throwable e) {
        LOGGER.error("Failed to cancel", e);
      }
      try {
        remoteConnection.abort(Runnable::run);
        remoteConnection = null;
      } catch (final Throwable e) {
        LOGGER.error("Failed to close", e);
      }
    }
  }

  /**
   * Interprets remoteStatement.
   * <p>
   * Notice that interpreter should be alive before calling interpreter. {@link AbstractJDBCInterpreter#isAlive()}
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

    final int rowLimit = Integer.parseInt(configuration.getOrDefault(QUERY_MAX_SAVE_ROW_LIMIT_KEY, "0"));
    try {
      this.remoteStatement = remoteConnection.createStatement();
      this.remoteStatement.setMaxRows(Integer.parseInt(configuration.getOrDefault(QUERY_ROWLIMIT_KEY, "0")));
      this.remoteStatement.setQueryTimeout(Integer.parseInt(configuration.getOrDefault(QUERY_TIMEOUT_KEY, "0")));
    } catch (final Exception ex) {
      return new InterpreterResult(Code.ERROR, new Message(Type.TEXT, ex.getMessage()));
    }


    final String precode = configuration.get("remoteStatement.precode");
    if (StringUtils.isNotBlank(precode)) {
      try {
        execQuery(JDBCInterpolation.interpolate(precode, params));
      } catch (final Exception e) {
        return new InterpreterResult(Code.ERROR, new Message(Type.TEXT, e.getMessage()));
      }
    }


    Code resultCode = Code.SUCCESS;
    final List<Message> resultMessages = new LinkedList<>();
    try {

      h2Manager.setConnection(noteContext.get(NoteContext.Z_ENV_NOTES_STORE_PATH.name()),
              noteContext.get(NoteContext.Z_ENV_NOTE_UUID.name()));
      final String query = prepareQueryToExecute(JDBCInterpolation.interpolate(st, params));
      final Iterator<String> iterator = simpleStatementIterator(query);
      final Iterator<String> statementIterator = getStatementIterator(query);

      while (statementIterator.hasNext()) {
        boolean results = this.remoteStatement.execute(statementIterator.next());

        while (results || this.remoteStatement.getUpdateCount() != -1) {
          if (iterator.hasNext()) {

            resultMessages.addAll(executeQuery(iterator.next(),
                    noteContext,
                    rowLimit,
                    this.remoteStatement.getResultSet(),
                    this.remoteStatement.getUpdateCount(),
                    this.remoteStatement.getMaxRows()));

          } else {
            resultMessages.add(new Message(Type.TEXT, "Error on query processing.\n"));
            throw new Exception();
          }
          results = this.remoteStatement.getMoreResults();
        }
      }
    } catch (final Exception ex) {
      resultMessages.add(new Message(Type.TEXT, ExceptionUtils.getStackTrace(ex)));
      resultCode = Code.ERROR;
      return new InterpreterResult(resultCode, resultMessages);
    } finally {
      try {
        h2Manager.releaseConnection();
      } catch (final Exception ex) {
        //SKIP
      }
    }


    final String postCode = configuration.get("remoteStatement.postcode");
    if (StringUtils.isNotBlank(postCode)) {
      try {
        execQuery(JDBCInterpolation.interpolate(postCode, params));
      } catch (final Throwable th) {
        resultMessages.add(new Message(Type.TEXT, ExceptionUtils.getStackTrace(th)));
        resultCode = Code.ERROR;
        return new InterpreterResult(resultCode, resultMessages);
      }
    }
    return new InterpreterResult(resultCode, resultMessages);
  }

  /**
   * Util method to execute a single remoteStatement.
   *
   * @param query - Query to execute, may consist of multiple statements, never {@code null}.
   * @return Result of remoteStatement execution, never {@code null}.
   */
  @Nonnull
  private List<Message> executeQuery(String query,
                                     @Nonnull final Map<String, String> noteContext,
                                     final int rowLimit,
                                     final ResultSet resultSet,
                                     final int updateCount,
                                     final int maxRows
                                     //,final boolean processResult
  ) throws SQLException {
    // ZP-142 WRONG
    //final String errorMessage = JDBCUtil.checkSyntax(queryString);
    //if (errorMessage != null) {
    //  return new InterpreterResult(Code.ERROR, new Message(Type.TEXT, errorMessage));
    //}
    final List<Message> resultMessages = new LinkedList<>();
    Matcher matcher = TBL_NAME_PATTERN.matcher(query);
    String tblName = matcher.find()
            ? matcher.group(1).replace("--#localh2.", "")
            : null;
    query = query.replace("--#localh2.", "--");

    //delete comments
    String statementCopy = query.toLowerCase();
    matcher = LINE_COMMENTS_PATTERN.matcher(statementCopy);
    statementCopy = matcher.replaceAll(StringUtils.EMPTY);
    matcher = BLOCK_COMMENTS_PATTERN.matcher(statementCopy);
    statementCopy = matcher.replaceAll(StringUtils.EMPTY);

    //find createTableName to copy remoteStatement result-table as virtual table in h2
    matcher = CREATE_TABLE_PATTERN.matcher(statementCopy);
    final String createTableName = matcher.find()
            ? matcher.group(10)
            : null;

    //find deleteTableName to delete existed virtual table in h2
    matcher = DELETE_TABLE_PATTERN.matcher(statementCopy);
    final String deleteTableName = matcher.find()
            ? matcher.group(9)
            : null;

    if (resultSet != null) {
      //create real table in h2 if remoteStatement.getResultSet() not null and tblName is not null
      final Message info;
      if (tblName == null) {
        tblName = noteContext.get(NoteContext.Z_ENV_PARAGRAPH_ID.name()) + "_"
                + String.valueOf(query.hashCode()).replaceAll("-", "_");
        final H2Table h2Table = h2TableConverter.resultSetToTable(resultSet,
                tblName,
                H2TableType.SELECT,
                -1,
                maxRows);
        info = h2Manager.saveH2Table(h2Table);
        h2Manager.saveMetaTable(h2Table);
      } else {
        final H2Table h2Table = h2TableConverter.resultSetToTable(resultSet,
                tblName,
                H2TableType.REAL,
                -1,
                rowLimit);
        info = h2Manager.saveH2Table(h2Table);
        h2Manager.saveMetaTable(h2Table);
      }
      resultMessages.add(info);
    } else if (updateCount != -1 && createTableName != null) {
      //create virtual table in h2 if createTableName not null
      final H2Table h2Table = h2TableConverter.resultSetToTable(
              remoteConnection.createStatement()
                      .executeQuery("SELECT * FROM " + createTableName + " LIMIT 100;"),
              createTableName,
              H2TableType.VIRTUAL,
              updateCount,
              100);
      final Message info = h2Manager.saveH2Table(h2Table);
      h2Manager.saveMetaTable(h2Table);
      resultMessages.add(info);

      resultMessages.add(new Message(Type.TEXT,
              "Query executed successfully. Affected rows: " + updateCount));

    } else if (updateCount != -1 && deleteTableName != null) {
      //delete virtual table from h2 if deleteTableName not null
      final String info = h2Manager.deleteTable(H2TableType.VIRTUAL.getSchemaName(),
              deleteTableName);
      resultMessages.add(new Message(Type.TEXT, info));
    } else {
      resultMessages.add(new Message(Type.TEXT,
              "Query executed successfully. Affected rows: " + updateCount));
    }
    return resultMessages;
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


  private String prepareQueryToExecute(String query) {
    final Matcher usedTablesMatcher = LOCAL_TBL_NAME_PATTERN.matcher(query);
    while (usedTablesMatcher.find()) {
      final String match = usedTablesMatcher.group(1);
      final String tableName = match.replace("local.", "");
      final ResultSet copyFromH2ResultSet =
              h2Manager.getTable(H2TableType.REAL.getSchemaName() + "." + tableName);
      // TODO: FIX SCHEMA NAME
      copyTableToInnerDB("PUBLIC", tableName, copyFromH2ResultSet);
      query = query.replace(match, tableName);
    }
    return query;
  }


  private void copyTableToInnerDB(final String schema,
                                  final String tableName,
                                  final ResultSet resultSet) {

    if (tableName.isEmpty()) {
      return;
    }
    try {
      remoteConnection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s.%s;", schema, tableName));
      final ResultSetMetaData md = resultSet.getMetaData();
      final LinkedHashMap<String, String> columns = h2TableConverter.getColumns(md);
      createTable(schema, tableName, columns);
      insertData(schema,
              tableName,
              resultSet,
              columns);

    } catch (final Exception ex) {
      LOGGER.info("Can't copy table from h2 to current db");
    }
  }

  private void createTable(final String schemaName,
                           final String tableName,
                           final LinkedHashMap<String, String> columns) throws SQLException {
    final String payload = columns.entrySet().stream()
            .map(v -> String.format("%s %s", v.getKey(), v.getValue()))
            .collect(Collectors.joining(",\n"));

    final String createTableScript = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s);",
            schemaName,
            tableName,
            payload);
    remoteConnection.createStatement().execute(createTableScript);
  }


  private void insertData(final String schema,
                          final String tableName,
                          final ResultSet resultSet,
                          final HashMap<String, String> columns) {
    final String preparedQueryScript = String.format(
            "INSERT INTO %s.%s (%s) VALUES (%s);",
            schema,
            tableName,
            String.join(", ", columns.keySet()),
            String.join(",", Collections.nCopies(columns.size(), "?"))
    );
    try (final PreparedStatement insertValuesStatement = remoteConnection.prepareStatement(preparedQueryScript)) {

      while (resultSet.next()) {
        for (int i = 1; i <= columns.keySet().size(); i++) {
          insertValuesStatement.setObject(i, resultSet.getObject(i));
        }
        insertValuesStatement.addBatch();
      }
      insertValuesStatement.executeBatch();
    } catch (final Exception ex) {
      //SKIP
    }
  }

  private Iterator<String> simpleStatementIterator(@Nonnull final String statements) {
    return Arrays.stream(statements.split(";"))
            .filter(s -> !s.trim().isEmpty())
            .collect(Collectors.toList())
            .iterator();
  }
}



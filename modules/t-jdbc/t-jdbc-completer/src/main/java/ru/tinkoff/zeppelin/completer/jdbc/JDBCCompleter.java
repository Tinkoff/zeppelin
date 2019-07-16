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
package ru.tinkoff.zeppelin.completer.jdbc;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.commons.lang3.StringUtils;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.zeppelin.commons.jdbc.utils.JDBCInstallation;
import ru.tinkoff.zeppelin.commons.jdbc.utils.JDBCInterpolation;
import ru.tinkoff.zeppelin.interpreter.Completer;
import ru.tinkoff.zeppelin.interpreter.InterpreterCompletion;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This is a completer for jdbc interpreter based on JSqlParser and Apache Metamodel.
 */
public class JDBCCompleter extends Completer {

  /**
   * Represents table element
   */
  private class TableName {
    /**
     * Full qualified name of an element,
     */
    final String key;

    /**
     * Name used in query, e.g. "select * from public.bug as b" -> key = public.bug, value = b.
     */
    final String value;

    TableName(final String key, final String value) {
      this.key = key;
      this.value = value;
    }

    String getKey() {
      return key;
    }

    String getValue() {
      return value;
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(JDBCCompleter.class);
  private static final List<String> metaRequiredKeyWords = Arrays.asList("where", "on", "and", "or", "not", "select", "by", "=", ">", "<");

  private static final String CONNECTION_USER_KEY = "connection.user";
  private static final String CONNECTION_URL_KEY = "connection.url";
  private static final String CONNECTION_PASSWORD_KEY = "connection.password";

  private static final String DRIVER_CLASS_NAME_KEY = "driver.className";
  private static final String DRIVER_ARTIFACT_KEY = "driver.artifact";
  private static final String DRIVER_ARTIFACT_DEPENDENCY = "driver.artifact.dependency";
  private static final String DRIVER_MAVEN_REPO_KEY = "driver.maven.repository.url";

  private final static Gson gson = new Gson();

  @SuppressWarnings("unused")
  public JDBCCompleter() {
    super();
  }

  // Simple database meta: schema -> table -> list of columns.
  // All elements must be sorted in the natural order for effective key retrieval by prefix.
  private static NavigableMap<String, NavigableMap<String, SortedSet<String>>> database = null;

  @Override
  public boolean isAlive() {
    return database != null;
  }

  @Override
  public boolean isOpened() {
    return database != null;
  }

  /**
   * Installs driver if needed and opens the database connection.
   *
   * @param configuration interpreter configuration.
   * @param classPath     class path.
   */
  @Override
  public void open(@Nonnull final Map<String, String> configuration, @Nonnull final String classPath) {
    synchronized (JDBCCompleter.class) {
      if (database != null) {
        return;
      }

      if (StringUtils.isEmpty(configuration.get("metadata.server.url"))) {
        // reliably
        loadMetadataUseMetamodel(configuration);
      } else {
        try {
          // fast
          loadMetadataFromMetaserver(configuration);
        } catch (final Exception e) {
          loadMetadataUseMetamodel(configuration);
        }
      }
    }
  }

  /**
   * Installs driver if needed and opens the database connection.
   *
   * @param configuration interpreter configuration.
   */
  private void loadMetadataUseMetamodel(@Nonnull final Map<String, String> configuration) {
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
          for (final File file : Objects.requireNonNull(driverFolder.listFiles())) {
            final URL url = file.toURI().toURL();

            urls.add(new URL("jar:" + url.toString() + "!/"));
          }

          final URLClassLoader classLoader = URLClassLoader.newInstance(urls.toArray(new URL[0]));
          final Class driverClass = Class.forName(className, true, classLoader);
          final Driver driver = (Driver) driverClass.newInstance();

          final Properties authSettings = new Properties();
          authSettings.put("user", user);
          authSettings.put("password", password);
          final Connection connection = driver.connect(dbUrl, authSettings);

          final Class dContent = Class.forName("org.apache.metamodel.jdbc.JdbcDataContext", true, classLoader);
          //noinspection unchecked
          final Constructor dConstructor = dContent.getConstructor(Connection.class);
          final JdbcDataContext dataContext = (JdbcDataContext) dConstructor.newInstance(connection);

          final NavigableMap<String, NavigableMap<String, SortedSet<String>>> result = new TreeMap<>();
          for (final Schema s : dataContext.getSchemas()) {
            for (final Table t : s.getTables()) {
              NavigableMap<String, SortedSet<String>> tableNode = result.get(s.getName());
              if (tableNode == null) {
                result.put(s.getName(), new TreeMap<>());
                tableNode = result.get(s.getName());
              }
              SortedSet<String> columns = tableNode.get(t.getName());
              if (columns == null) {
                tableNode.put(t.getName(), new TreeSet<>());
                columns = tableNode.get(t.getName());
              }
              columns.addAll(t.getColumns().stream().map(Column::getName).collect(Collectors.toList()));
            }
          }
          dataContext.close(connection);
          connection.close();

          database = result;
        } catch (final Exception e) {
          LOGGER.error("SQL driver configured incorrectly", e);
        }
      }
    }
  }

  private void loadMetadataFromMetaserver(final Map<String, String> configuration) throws Exception {
    final String metaserverUrl = configuration.get("metadata.server.url");
    final String metaserverDBName = configuration.get("metadata.server.database_name");
    final String metaserverEndpoint = String.format("%s/%s/snapshot", metaserverUrl, metaserverDBName);

    final HttpURLConnection conn = (HttpURLConnection) new URL(metaserverEndpoint).openConnection();
    conn.setRequestMethod("GET");

    final StringBuilder sb = new StringBuilder();
    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      while (reader.ready()) {
        sb.append(reader.readLine());
      }
    }

    // [schemaName : [tableName : [columnName]]]
    final Type type = new TypeToken<Map<String, Map<String, List<String>>>>() {}.getType();
    final Map<String, Map<String, List<String>>> meta = gson.fromJson(sb.toString(), type);

    final NavigableMap<String, NavigableMap<String, SortedSet<String>>> result = new TreeMap<>();
    for (final Map.Entry<String, Map<String, List<String>>> schemaEntry : meta.entrySet()) {
      for (final Map.Entry<String, List<String>> tableEntry : schemaEntry.getValue().entrySet()) {
        NavigableMap<String, SortedSet<String>> tableNode = result.get(schemaEntry.getKey());
        if (tableNode == null) {
          result.put(schemaEntry.getKey(), new TreeMap<>());
          tableNode = result.get(schemaEntry.getKey());
        }
        SortedSet<String> columns = tableNode.get(tableEntry.getKey());
        if (columns == null) {
          tableNode.put(tableEntry.getKey(), new TreeSet<>());
          columns = tableNode.get(tableEntry.getKey());
        }
        columns.addAll(tableEntry.getValue());
      }
    }
    database = result;
  }

  @Override
  public boolean isReusableForConfiguration(@Nonnull final Map<String, String> configuration) {
    return true;
  }

  /**
   * May be called from another thread.
   **/
  @Override
  public void cancel() { }

  /**
   * May be called from another thread.
   */
  @Override
  public void close() { }

  @Override
  public String complete(final String st,
                         final int cursorPosition,
                         final Map<String, String> noteContext,
                         final Map<String, String> userContext,
                         final Map<String, String> configuration) {

    final Map<String, String> params = new HashMap<>();
    params.putAll(noteContext);
    params.putAll(userContext);

    final JDBCInterpolation.InterpolateResponse response = JDBCInterpolation.interpolate(st, cursorPosition, params);

    try {
      final Set<InterpreterCompletion> result = complete(response.getPayload(), response.getCursorPosition());
      return new Gson().toJson(result);
    } catch (final Exception e) {
      return new Gson().toJson(Collections.emptySet());
    }
  }

  /**
   * Creates autocomplete list.
   *
   * @param buffer full text, where completion called.
   * @param pos cursor position
   * @return completion result
   */
  private SortedSet<InterpreterCompletion> complete(@Nonnull final String buffer, final int pos) {
    final SortedSet<InterpreterCompletion> completions = new TreeSet<>();

    // 1. Buffer preprocessing.
    // 1.1 If multiple statements passed - extract cursor statement (statement ends on cursor position)
    // e.g "select * from a; select * from !POS! b;" -> "select * from "
    final String statement = getStatement(buffer, pos);

    // 1.2 Get the whole cursor statement
    // e.g "select * from a; select * from !POS! b;" -> "select * from b;"
    int last = buffer.lastIndexOf(';');
    if (last < pos) {
      last = buffer.length();
    }
    final String wholeStatement = getStatement(buffer, last);

    // 1.3 Get the cursor word and previous word.
    // if cursor is pointed to space - cursor word is null.
    String cursorWord = null;
    String previousWord = null;
    if (pos > 0 && !buffer.isEmpty()) {
      final List<String> words = Arrays.asList(statement.split("\\s+"));
      if (buffer.charAt(pos - 1) != ' ') {
        cursorWord = words.get(words.size() - 1);
        if (words.size() - 2 >= 0) {
          previousWord = words.get(words.size() - 2);
        }
      } else {
        previousWord = words.get(words.size() - 1);
      }
    }

    // 1.4 Get tables used in whole statement.
    final Set<TableName> tableList =
        getTablesFromWholeStatement(
            wholeStatement,
            previousWord != null && metaRequiredKeyWords.contains(previousWord.toLowerCase())
        );
    try {
      // 2. If statement is incorrect - exception would be thrown
      CCJSqlParserUtil.parse(statement);

      // 3. If statement is correct.
      // 3.1 Process default completion list.
      completeWithCandidates(
          new TreeSet<>(
              Arrays.asList(
                  "from", "where", "select", "join", "left", "right", "inner", "outer", "on", "and", "or"
              )
          ),
          cursorWord,
          completions,
          "keyword",
          null
      );
      // 3.2 Add meta.
      completeWithMeta(cursorWord, completions, tableList);
    } catch (final JSQLParserException e) {
      if (e.getCause().toString().contains("Was expecting one of:")) {
        // 2.1 Get expected keywords from exception.
        final List<String> expected = Arrays.asList(e.getCause().toString()
                .substring(e.getCause().toString().indexOf("Was expecting one of:")).split("\n"));
        // 2.2 Check if meta is required or not.
        final boolean loadMeta = expected.contains("    <S_IDENTIFIER>")
            || (cursorWord != null && cursorWord.contains("."))
            || (previousWord != null && (previousWord.equals("where") || previousWord.equals("on")
            || previousWord.equals("=")));

        // 2.3 Clean expected list.
        final SortedSet<String> prepared = expected.subList(1, expected.size())
                .stream()
                .map(v -> v.trim().replace("\"", "").toLowerCase())
                .filter(v -> !v.isEmpty() && !v.startsWith("<") && !v.startsWith("{"))
                .collect(Collectors.toCollection(TreeSet::new));

        // 2.4 Complete with expected keywords.
        completeWithCandidates(prepared, cursorWord, completions, "keyword", null);
        if (loadMeta) {
          // 2.5 Complete with meta if needed.
          completeWithMeta(cursorWord, completions, tableList);
        }
      }
    }

    return completions;
  }

  /**
   * Gets used table names using JSQLParser.
   *
   * @param statement Statement to get names from, never {@code null}.
   * @return Set of table names used in statement.
   */
  private Set<TableName> getTablesFromWholeStatement(@Nonnull final String statement, final boolean meta) {
    String statementToParse = statement;

    // Try to get names for 3 times, on each step if failed to parse - trying to fix query and rerun search.
    for (int i = 0; i < 3; ++i) {
      final Statement parseExpression;
      try {
        parseExpression = CCJSqlParserUtil.parse(statementToParse);
      } catch (final JSQLParserException e) {
        final List<String> expected = Arrays.asList(e.getCause().toString()
            .substring(e.getCause().toString().indexOf("Was expecting one of:")).split("\n"));
        final boolean loadMeta = expected.contains("    <S_IDENTIFIER>");

        if (loadMeta) {
          // fix query - add "S" to place where <S_IDENTIFIER> is needed.
          final String errorMsg = e.getCause().toString();
          final String columnPrefix = errorMsg.substring(errorMsg.indexOf("column ") + "column ".length());
          final int errorPos = Integer.parseInt(columnPrefix.substring(0, columnPrefix.indexOf('.')));

          statementToParse = String.join(" ",
              statementToParse.substring(0, errorPos - 1),
              "S",
              statementToParse.substring(errorPos - 1)
          );
          continue;
        }

        if (meta) {
          // if meta is needed but <S_IDENTIFIER> wasn't expected - add "S" to end of the query.
          statementToParse += " S";
          continue;
        }

        // faced with another exception - return empty list.
        return Collections.emptySet();
      }

      // getting names
      final HashSet<TableName> result = new HashSet<>();
      final TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
      tablesNamesFinder.getTableList(parseExpression).forEach(n -> result.add(new TableName(n, n)));
      // getting aliases
      try {
        final Select select = (Select) parseExpression;
        final PlainSelect inner = (PlainSelect) select.getSelectBody();
        addAliases(inner.getFromItem(), result);
        if (inner.getJoins() != null) {
          for (final Join join : inner.getJoins()) {
            addAliases(join.getRightItem(), result);
          }
        }
      } catch (final Exception e) {
        // skip
      }
      return  result;
    }
    return Collections.emptySet();
  }

  private void addAliases(@Nullable final FromItem item,
                          @Nonnull final HashSet<TableName> names) {
    if (item == null) {
      return;
    }
    final Alias alias = item.getAlias();
    if (alias != null) {
      if (item.toString().toLowerCase().contains("as")) {
        names.add(
            new TableName(
                item.toString()
                    .substring(0, item.toString().toLowerCase().indexOf("as"))
                    .trim(),
                alias.getName()
            )
        );
      } else {
        names.add(
            new TableName(
                item.toString()
                    .substring(0, item.toString().toLowerCase().indexOf(' '))
                    .trim(),
                alias.getName()
            )
        );
      }
    }
  }

  /**
   * Splits buffer by ";" and returns cursor statement (statement ends on cursor pos).
   * e.g "select * from a; select * from !POS! b;" -> "select * from "
   *
   * @param buffer Whole sql script, never {@code null}.
   * @param pos Cursor position.
   * @return Statement which cursor is pointed to.
   */
  private String getStatement(@Nonnull final String buffer, final int pos) {
    String statement;
    if (buffer.contains("/*") && buffer.contains("*/") && pos > buffer.indexOf("/*")) {
      // if buffer contains "/**/" comments - cut buffer with whole commented block
      final int endOfCommentBLock = buffer.lastIndexOf("*/") + 2;
      // if comment block ends after pos - cut buffer with the whole block to clean it using regexp
      statement = buffer.substring(0, endOfCommentBLock > pos ? endOfCommentBLock : pos)
          .replaceAll("--.*|(\"(?:\\\\[^\"]|\\\\\"|.)*?\")|(?s)/\\*.*?\\*/", "");
    } else {
      statement = buffer.substring(0, pos).replaceAll("--.*", "");
    }
    statement = statement.replaceAll("\\s+", " ");
    final List<String> statements = Arrays.asList(statement.split(";"));
    if (statements.isEmpty()) {
      return buffer;
    }
    return statements.get(statements.size() - 1);
  }

  /**
   * Fills the autocomplete list with database object names (schemas, tables, columns).
   *
   * @param cursorWord, word on which the cursor, {@code null} if cursor on space.
   * @param completions, collection to fill, nevet {@code null}
   */
  private void completeWithMeta(@Nullable String cursorWord,
                                @Nonnull final Set<InterpreterCompletion> completions,
                                @Nonnull final Set<TableName> tables) {
    // 1. If cursor on space
    if (cursorWord == null) {
      // 1.1 Complete with schemas.
      completeWithCandidates(database.navigableKeySet(), null, completions, "schema", null);

      // 1.2 If tables exists - complete with columns from each table.
      if (!tables.isEmpty()) {
        for (final TableName name : tables) {
          if (name.getKey().contains(".")) {
            final List<String> nodes = Arrays.asList(name.getKey().split("\\."));
            completeWithCandidates(
                database.get(nodes.get(0)).get(nodes.get(1)),
                cursorWord, completions,
                "column",
                nodes.get(0) + "." + nodes.get(1)
            );
          }
        }
      }
      return;
    }

    for (final TableName name : tables) {
      if (cursorWord.equals(name.getValue()) || cursorWord.equals(name.getValue() + ".")) {
        cursorWord = name.getKey() + (cursorWord.endsWith(".") ? "." : "");
        break;
      }
    }

    // 2. If cursor on word.
    // ASSUMPTION: users use fully qualified names [schema].[table/view].[column]).
    // 2.1 split word on nodes, e.g. public.bug -> ['public', 'bug'] to get schema and table names.
    final List<String> nodes = Arrays.asList(cursorWord.split("\\."));
    final String lastNode = nodes.get(nodes.size() - 1);
    int nodeCnt = nodes.size();
    if (cursorWord.endsWith(".")) {
      // Case: public.bug -> ['public', ''], table name is empty.
      nodeCnt += 1;
    }

    if (nodeCnt == 1) {
      // 2.2 if there is one node - user started to write schema name (see assumption above)
      completeWithCandidates(database.navigableKeySet(), lastNode, completions, "schema", null);
    } else if (nodeCnt == 2) {
      // 2.3 if there are two nodes - user started to write table name (see assumption above)
      if (nodes.size() == 1) {
        // 2.3.1 if there is empty table name.
        // e.g. cursorWord was 'public.', schema is written, but table name is empty
        // fill with names of tables from the corresponding schema.
        if (database.get(nodes.get(0)) != null) {
          completeWithCandidates(database.get(nodes.get(0)).navigableKeySet(), null, completions,
              "table", nodes.get(0));
        }

        // 2.3.2 BREAK ASSUMPTION: if first node is table name - user started to write column name.
        database.forEach((key, value) -> {
          if (value.get(nodes.get(0)) != null) {
            completeWithCandidates(value.get(nodes.get(0)), null, completions, "column", key + "." + nodes.get(0));
          }
        });
      } else {
        // 2.3.3 table name is not empty.
        if (database.get(nodes.get(0)) != null) {
          completeWithCandidates(database.get(nodes.get(0)).navigableKeySet(), lastNode, completions, "table", nodes.get(0));
        }
      }
    } else if (nodeCnt == 3) {
      // 2.4 if there are three nodes - user started to write column name (see assumption above)
      if (nodes.size() == 2) {
        // 2.4.1 if there is empty column name.
        // e.g. cursorWord was 'public.bug.', schema and table is written, but column name is empty
        // fill with names of columns from the corresponding schema and table.
        if (database.get(nodes.get(0)) != null && database.get(nodes.get(0)).get(nodes.get(1)) != null) {
          completeWithCandidates(database.get(nodes.get(0)).get(nodes.get(1)), null, completions, "column", nodes.get(0) + "." + nodes.get(1));
        }
      } else {
        // 2.4.2 column name is not empty.
        if (database.get(nodes.get(0)) != null && database.get(nodes.get(0)).get(nodes.get(1)) != null) {
          completeWithCandidates(database.get(nodes.get(0)).get(nodes.get(1)), lastNode, completions, "column", nodes.get(0) + "." + nodes.get(1));
        }
      }
    }
  }

  /**
   * Adds candidates to completion list.
   *
   * @param candidates, set of matched words.
   * @param prefix, prefix to match, {@code null} if no prefix
   * @param completions collection to fill
   * @param type completion type
   */
  private void completeWithCandidates(@Nonnull final SortedSet<String> candidates,
                                      @Nullable final String prefix,
                                      @Nonnull final Set<InterpreterCompletion> completions,
                                      @Nonnull final String type,
                                      @Nullable final String fullNamePrefix) {
    if (prefix != null) {
      completeTailSet(candidates.tailSet(prefix), prefix, completions, type, fullNamePrefix);
    } else {
      completions.addAll(
          candidates.stream().map(t -> createInterpreterCompletion(t, t, type, fullNamePrefix))
          .collect(Collectors.toList()));
    }
  }

  /**
   * Adds words with the same prefix to completion.
   *
   * @param tailSet, set of matched words.
   * @param prefix, prefix to match.
   * @param completions collection to fill
   * @param type completion type
   */
  private void completeTailSet(@Nonnull final SortedSet<String> tailSet,
                               @Nonnull final String prefix,
                               @Nonnull final Set<InterpreterCompletion> completions,
                               @Nonnull final String type,
                               @Nullable final String fullNamePrefix) {
    for (final String match : tailSet) {
      if (!match.startsWith(prefix)) {
        // tailSet is sorted, so if the current element does not begin with this prefix,
        // all of the following elements will also have a different prefix.
        break;
      }
      completions.add(createInterpreterCompletion(match, match, type, fullNamePrefix));
    }
  }

  private final Set<String> preferredKeywords = new HashSet<>(
      Arrays.asList("select", "*", "join", "from", "update", "insert", "delete", "with", "set", "on"));

  private InterpreterCompletion createInterpreterCompletion(@Nonnull final String name,
                                                            @Nonnull final String value,
                                                            @Nonnull final String meta,
                                                            @Nullable final String prefix) {
    final int score;
    switch (meta) {
      case "column":
        score = 900;
        break;
      case "table":
        score = 700;
        break;
      case "schema":
        score = 500;
        break;
      default:
        score = preferredKeywords.contains(value) ? 400 : 300;
    }

    String description = name;
    if (prefix != null) {
      description = prefix + "." + name;
    }
    return new InterpreterCompletion(name, value, meta, description, score);
  }
}
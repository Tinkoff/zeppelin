package ru.tinkoff.zeppelin.engine;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.content.Content;
import ru.tinkoff.zeppelin.core.content.ContentType;
import ru.tinkoff.zeppelin.storage.ContentDAO;
import ru.tinkoff.zeppelin.storage.ContentParamDAO;
import ru.tinkoff.zeppelin.storage.NoteDAO;

import java.io.File;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class H2MonitoringService {
  private static final Logger LOG = LoggerFactory.getLogger(H2MonitoringService.class);

  private final NoteDAO noteDAO;
  private final ContentDAO contentDAO;
  private final ContentParamDAO contentParamDAO;

  @Autowired
  public H2MonitoringService(final NoteDAO noteDAO,
                             final ContentDAO contentDAO,
                             final ContentParamDAO contentParamDAO) {
    this.noteDAO = noteDAO;
    this.contentDAO = contentDAO;
    this.contentParamDAO = contentParamDAO;
  }

  public void scanH2(final long noteId) {
    final String noteContextPath =
            Configuration.getNoteStorePath()
                    + File.separator
                    + noteDAO.get(noteId).getUuid()
                    + File.separator
                    + "outputDB";
    try (final Connection con = DriverManager.getConnection(
            "jdbc:h2:file:" + Paths.get(noteContextPath).normalize().toFile().getAbsolutePath(),
            "sa",
            "")) {

      compareH2ToContext(con, noteContextPath, noteId);
    } catch (final Throwable th) {
      //Error or schema V_TABLE with virtual tables not exist
      LOG.info(th.getMessage());
    }
  }

  private void compareH2ToContext(final Connection connection, final String locationBase, final long noteId) throws SQLException, NullPointerException {

    final LinkedList<String> schemas = new LinkedList<>(Arrays.asList("V_TABLES", "R_TABLES"));
    for (final String schema: schemas) {
      //get table names for each schema
      final ResultSet resultSet = connection
              .createStatement()
              .executeQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema + "';");
      List<String> tables = new LinkedList<>();
      while (resultSet.next()) {
        tables.add(resultSet.getString("TABLE_NAME"));
      }
      tables = tables.stream().filter(s -> !s.toUpperCase().endsWith("_META")).collect(Collectors.toList());

      //load content
      final List<Content> contentList = contentDAO.getNoteContent(noteId);


      for (final String tableName : tables) {
        final ResultSet tableResultSet = connection.createStatement().executeQuery(String.format(
                "SELECT *  FROM %s.%s LIMIT 10;", schema, tableName));

        final ResultSetMetaData md = tableResultSet.getMetaData();

        final List<String> columns = new LinkedList<>();

        for (int i = 1; i < md.getColumnCount() + 1; i++) {

          final String createTable = (StringUtils.isNotEmpty(md.getColumnLabel(i))
                  ? md.getColumnLabel(i)
                  : md.getColumnName(i)) +
                  " \t" +
                  md.getColumnTypeName(i);
          columns.add(createTable);
        }
        final ResultSet rs = connection.createStatement().executeQuery(String.format(
                "SELECT ROW_COUNT FROM %s.%s_META WHERE TABLE_NAME = '%s';",schema , tableName , tableName.toUpperCase()));
        final long rows = !rs.next() ? 0L : rs.getLong("ROW_COUNT");

        final String location = locationBase + ":" + schema + "." + tableName;
        addContent(noteId, contentList, columns, location, rows);
      }
    }
  }

  private void addContent(final long noteId, final List<Content> contentList, final List<String> columns, final String location, final Long rows) {
    contentList.stream()
            .filter(j -> location.equals(j.getLocation()))
            .findFirst().ifPresent(contentDAO::remove);
    try {
      contentDAO.persist(new Content(noteId, ContentType.TABLE, rows.toString(), location, null));
      final Content content = contentDAO.getContentByLocation(location);
      if (content != null) {
        contentParamDAO.persist(content.getId(), "TABLE_COLUMNS", new Gson().fromJson(new Gson().toJson(columns), JsonElement.class));
      }
    } catch (final Exception ex) {
      LOG.info(ex.getMessage());
    }
  }

  private void getVTables(final Connection connection,
                          final String locationBase,
                          final long noteId) throws SQLException {
    if (connection.createStatement()
            .executeQuery("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'V_TABLE'") != null) {
      if (connection.createStatement().executeQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES" +
              " WHERE TABLE_SCHEMA = 'V_TABLE' AND TABLE_NAME = 'TABLES'") != null) {
        final ResultSet resultSet = connection.createStatement()
                .executeQuery("SELECT * FROM V_TABLES.TABLES");
        final List<Content> contentList = contentDAO.getNoteContent(noteId);

        while (resultSet.next()) {
          final String name = resultSet.getString("TABLE_NAME");
          final Long rows = resultSet.getLong("ROWS");
          final ArrayList<String> columns = new ArrayList<>();
          columns.addAll(Arrays.asList(resultSet.getString("COLUMNS").split("\n")));
          final String location = locationBase + ":V_TABLES." + name;
          addContent(noteId, contentList, columns, location, rows);
        }
      }
    }
  }
}

package ru.tinkoff.zeppelin.interpreter.jdbc.gp.analyzer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.tinkoff.zeppelin.commons.jdbc.analyzer.ResourceDAO;
import ru.tinkoff.zeppelin.commons.jdbc.analyzer.ResourceDTO;

class GPResourceGroupActivityDAO implements ResourceDAO {

  private static final String GET_STATS = "" +
      "SELECT datid,\n" +
      "       datname,\n" +
      "       procpid,\n" +
      "       sess_id,\n" +
      "       usesysid,\n" +
      "       usename,\n" +
      "       current_query,\n" +
      "       waiting,\n" +
      "       query_start,\n" +
      "       backend_start,\n" +
      "       client_addr,\n" +
      "       client_port,\n" +
      "       application_name,\n" +
      "       xact_start,\n" +
      "       waiting_reason,\n" +
      "       rsgid,\n" +
      "       rsgname,\n" +
      "       rsgqueueduration" +
      "FROM pg_stat_activity;";

  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  GPResourceGroupActivityDAO(final DataSource dataSource) {
    this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
  }

  private static GPResourceGroupActivityDTO mapRow(final ResultSet resultSet, final int i) throws SQLException {
    final long datid = resultSet.getLong("datid");
    final String datname = resultSet.getString("datname");
    final int procpid = resultSet.getInt("procpid");
    final int sess_id = resultSet.getInt("sess_id");
    final long usesysid = resultSet.getLong("usesysid");
    final String usename = resultSet.getString("usename");
    final String current_query = resultSet.getString("current_query");
    final boolean waiting = resultSet.getBoolean("waiting");

    final LocalDateTime query_start = null != resultSet.getTimestamp("query_start")
        ? resultSet.getTimestamp("query_start").toLocalDateTime()
        : null;
    final LocalDateTime backend_start = null != resultSet.getTimestamp("backend_start")
        ? resultSet.getTimestamp("backend_start").toLocalDateTime()
        : null;

    final String client_addr = resultSet.getString("client_addr");
    final int client_port = resultSet.getInt("client_port");
    final String application_name = resultSet.getString("application_name");

    final LocalDateTime xact_start = null != resultSet.getTimestamp("xact_start")
        ? resultSet.getTimestamp("xact_start").toLocalDateTime()
        : null;

    final String waiting_reason = resultSet.getString("waiting_reason");
    final long rsgid = resultSet.getLong("rsgid");
    final String rsgname = resultSet.getString("rsgname");
    final long rsgqueueduration = resultSet.getLong("rsgqueueduration");


    return new GPResourceGroupActivityDTO(
        datid,
        datname,
        procpid,
        sess_id,
        usesysid,
        usename,
        current_query,
        waiting,
        query_start,
        backend_start,
        client_addr,
        client_port,
        application_name,
        xact_start,
        waiting_reason,
        rsgid,
        rsgname,
        rsgqueueduration
    );
  }

  @Override
  public List<ResourceDTO> getAllResourceInfo() {
    return namedParameterJdbcTemplate.query(GET_STATS, GPResourceGroupActivityDAO::mapRow);
  }
}

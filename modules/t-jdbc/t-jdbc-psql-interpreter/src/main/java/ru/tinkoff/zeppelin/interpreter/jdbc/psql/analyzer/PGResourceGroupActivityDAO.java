package ru.tinkoff.zeppelin.interpreter.jdbc.psql.analyzer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.tinkoff.zeppelin.commons.jdbc.analyzer.ResourceDAO;
import ru.tinkoff.zeppelin.commons.jdbc.analyzer.ResourceDTO;

class PGResourceGroupActivityDAO implements ResourceDAO {

  private static final String GET_STATS = "" +
      "SELECT datid,\n" +
      "       datname,\n" +
      "       pid,\n" +
      "       usesysid,\n" +
      "       usename,\n" +
      "       application_name,\n" +
      "       client_addr,\n" +
      "       client_hostname,\n" +
      "       client_port,\n" +
      "       backend_start,\n" +
      "       xact_start,\n" +
      "       query_start,\n" +
      "       state_change,\n" +
      "       wait_event_type,\n" +
      "       wait_event,\n" +
      "       state,\n" +
      "       backend_xid,\n" +
      "       backend_xmin,\n" +
      "       query,\n" +
      "       backend_type\n" +
      "FROM pg_stat_activity;";

  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  PGResourceGroupActivityDAO(final DataSource dataSource) {
    this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
  }

  private static PGResourceGroupActivityDTO mapRow(final ResultSet resultSet, final int i) throws SQLException {
    final long datid = resultSet. getLong("datid");
    final String datname = resultSet. getString("datname");
    final long pid = resultSet. getLong("pid");
    final long usesysid = resultSet. getLong("usesysid");
    final String usename = resultSet. getString("usename");
    final String application_name = resultSet. getString ("application_name");
    final String client_addr = resultSet. getString ("client_addr");
    final String client_hostname = resultSet. getString ("client_hostname");
    final long client_port = resultSet. getLong("client_port");

    final LocalDateTime backend_start = null != resultSet.getTimestamp("backend_start")
        ? resultSet.getTimestamp("backend_start").toLocalDateTime()
        : null;
    final LocalDateTime xact_start = null != resultSet.getTimestamp("xact_start")
        ? resultSet.getTimestamp("xact_start").toLocalDateTime()
        : null;
    final LocalDateTime query_start = null != resultSet.getTimestamp("query_start")
        ? resultSet.getTimestamp("query_start").toLocalDateTime()
        : null;
    final LocalDateTime state_change = null != resultSet.getTimestamp("state_change")
        ? resultSet.getTimestamp("state_change").toLocalDateTime()
        : null;

    final String wait_event_type = resultSet.getString("wait_event_type");
    final String wait_event = resultSet.getString("wait_event");
    final String state = resultSet.getString("state");
    final long backend_xid = resultSet.getLong("backend_xid");
    final long backend_xmin = resultSet.getLong("backend_xmin");
    final String query = resultSet.getString("query");
    final String backend_type = resultSet.getString("backend_type");

    return new PGResourceGroupActivityDTO(
        datid,
        datname,
        pid,
        usesysid,
        usename,
        application_name,
        client_addr,
        client_hostname,
        client_port,
        backend_start,
        xact_start,
        query_start,
        state_change,
        wait_event_type,
        wait_event,
        state,
        backend_xid,
        backend_xmin,
        query,
        backend_type
    );
  }

  @Override
  public List<ResourceDTO> getAllResourceInfo() {
    return namedParameterJdbcTemplate.query(GET_STATS, PGResourceGroupActivityDAO::mapRow);
  }
}

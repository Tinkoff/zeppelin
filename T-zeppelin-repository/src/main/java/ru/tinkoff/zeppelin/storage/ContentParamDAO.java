package ru.tinkoff.zeppelin.storage;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;

import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static ru.tinkoff.zeppelin.storage.Utils.generatePGjson;
import static ru.tinkoff.zeppelin.storage.Utils.getPGjson;

@Component
public class ContentParamDAO {
  private static final Logger LOG = LoggerFactory.getLogger(ContentDAO.class);


  private static final String GET_CONTENT_PARAMS = "" +
          "SELECT CONTENT_ID,\n" +
          "       NAME,\n" +
          "       VALUE\n" +
          "FROM CONTENT_PARAMS\n" +
          "WHERE CONTENT_ID = :CONTENT_ID;";


  private static final String PERSIST_CONTENT_PARAM = "" +
          "INSERT INTO CONTENT_PARAMS (CONTENT_ID,\n" +
          "                     NAME,\n" +
          "                     VALUE)\n" +
          "VALUES (:CONTENT_ID,\n" +
          "        :NAME,\n" +
          "        :VALUE);";


  private final NamedParameterJdbcTemplate jdbcTemplate;
  private static final Gson gson = new Gson();


  public ContentParamDAO(final NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }


  private static Map<String, JsonElement> ResultSetExtractor(final ResultSet resultSet) throws SQLException {
    HashMap<String, JsonElement> result = new HashMap<>();
    while (resultSet.next()) {
      final String name = resultSet.getString("NAME");
      result.put(name, gson.fromJson(resultSet.getString("VALUE"), JsonElement.class));
    }
    return result;
  }

  public void persist(final Long contentId,
                      final String name,
                      final JsonElement value) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("CONTENT_ID", contentId)
            .addValue("NAME", name)
            .addValue("VALUE", getPGjson(value));

    jdbcTemplate.update(PERSIST_CONTENT_PARAM, parameters);
  }


  public Map<String, JsonElement> getContentParams(final Long contentId) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("CONTENT_ID", contentId);

    return jdbcTemplate.query(
            GET_CONTENT_PARAMS,
            parameters,
            ContentParamDAO::ResultSetExtractor
    );
  }
}

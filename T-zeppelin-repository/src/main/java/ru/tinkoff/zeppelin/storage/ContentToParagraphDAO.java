package ru.tinkoff.zeppelin.storage;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.content.Content;
import ru.tinkoff.zeppelin.core.externalDTO.ContentToParagraphDTO;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;

@Component
public class ContentToParagraphDAO {

  private static final String GET_PARAGRAPH_CONTENT = "" +
    "SELECT CONTENT_ID,\n" +
    "       PARAGRAPH_ID\n" +
    "FROM CONTENT_TO_PARAGRAPH\n" +
    "WHERE PARAGRAPH_ID = :PARAGRAPH_ID;";

  private static final String GET_CONTENT_PARAGRAPHS = "" +
      "SELECT CONTENT_ID,\n" +
      "       PARAGRAPH_ID\n" +
      "FROM CONTENT_TO_PARAGRAPH\n" +
      "WHERE CONTENT_ID = :CONTENT_ID;";

  private static final String INSERT =  "" +
      "INSERT INTO CONTENT_TO_PARAGRAPH (CONTENT_ID,\n" +
      "                                  PARAGRAPH_ID)\n" +
      "VALUES (:CONTENT_ID,\n" +
      "        :PARAGRAPH_ID);";

  private final NamedParameterJdbcTemplate jdbcTemplate;


  public ContentToParagraphDAO(final NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  private static ContentToParagraphDTO mapRow(final ResultSet resultSet, final int i) throws SQLException {
    final long contentId = resultSet.getLong("CONTENT_ID");
    final long paragraphId = resultSet.getLong("PARAGRAPH_ID");
    return new ContentToParagraphDTO(contentId, paragraphId);
  }


  public ContentToParagraphDTO persist(@Nonnull final Content content,
                                       @Nonnull final Paragraph paragraph) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("CONTENT_ID", content.getDatabaseId())
        .addValue("PARAGRAPH_ID", paragraph.getId());

    jdbcTemplate.update(INSERT, parameters);
    return new ContentToParagraphDTO(content.getDatabaseId(), paragraph.getId());
  }


  @Nonnull
  public List<ContentToParagraphDTO> getParagraphContent(@Nonnull final Paragraph paragraph) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("PARAGRAPH_ID", paragraph.getId());

    return jdbcTemplate.query(
        GET_PARAGRAPH_CONTENT,
        parameters,
        ContentToParagraphDAO::mapRow
    );
  }

  @Nonnull
  public List<ContentToParagraphDTO> getContentParagraphs(@Nonnull final Content content) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("CONTENT_ID", content.getDatabaseId());

    return jdbcTemplate.query(
        GET_CONTENT_PARAGRAPHS,
        parameters,
        ContentToParagraphDAO::mapRow
    );
  }
}

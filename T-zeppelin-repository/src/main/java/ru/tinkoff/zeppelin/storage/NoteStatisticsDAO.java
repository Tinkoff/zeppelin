package ru.tinkoff.zeppelin.storage;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.notebook.JobBatch;
import ru.tinkoff.zeppelin.core.notebook.NoteStatisticInner;
import ru.tinkoff.zeppelin.core.notebook.NoteStatistic;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;

@Component
public class NoteStatisticsDAO {

    private static final String GET_USER_FAV_NOTES_STAT = "" +
            "SELECT DISTINCT ON (NOTES.ID, JOB.PARAGRAPH_ID)\n" +
            "       FAVORITE_NOTES.NOTE_UUID\n" +
            "      ,NOTES.ID as NOTE_ID\n" +
            "      ,JOB.PARAGRAPH_ID\n" +
            "      ,PARAGRAPHS.UUID as PARAGRAPH_UUID\n" +
            "      ,JOB.STARTED_AT\n" +
            "      ,JOB.ENDED_AT\n" +
            "      ,JOB.STATUS\n" +
            "      ,JOB.USER_NAME\n" +
            "FROM FAVORITE_NOTES\n" +
            "JOIN NOTES\n" +
            "       ON NOTES.UUID = FAVORITE_NOTES.NOTE_UUID\n" +
            "LEFT JOIN JOB_BATCH\n" +
            "       ON JOB_BATCH.NOTE_ID = NOTES.ID\n" +
            "JOIN JOB\n" +
            "       ON JOB.BATCH_ID = JOB_BATCH.ID\n" +
            "LEFT JOIN PARAGRAPHS\n" +
            "   ON PARAGRAPHS.ID = JOB.PARAGRAPH_ID\n" +
            "WHERE FAVORITE_NOTES.USER_NAME = :USER_NAME\n" +
            "ORDER BY NOTES.ID, JOB.PARAGRAPH_ID, JOB.STARTED_AT DESC;";

    private static final String GET_USER_RECENT_NOTES_STAT = "" +
            "SELECT DISTINCT ON (NOTES.ID, JOB.PARAGRAPH_ID)\n" +
            "       RECENT_NOTES.NOTE_UUID\n" +
            "      ,NOTES.ID as NOTE_ID\n" +
            "      ,JOB.PARAGRAPH_ID\n" +
            "      ,PARAGRAPHS.UUID as PARAGRAPH_UUID\n" +
            "      ,JOB.STARTED_AT\n" +
            "      ,JOB.ENDED_AT\n" +
            "      ,JOB.STATUS\n" +
            "      ,JOB.USER_NAME\n" +
            "FROM RECENT_NOTES\n" +
            "JOIN NOTES\n" +
            "       ON NOTES.UUID = RECENT_NOTES.NOTE_UUID\n" +
            "LEFT JOIN JOB_BATCH\n" +
            "       ON JOB_BATCH.NOTE_ID = NOTES.ID\n" +
            "JOIN JOB\n" +
            "       ON JOB.BATCH_ID = JOB_BATCH.ID\n" +
            "LEFT JOIN PARAGRAPHS\n" +
            "   ON PARAGRAPHS.ID = JOB.PARAGRAPH_ID\n" +
            "WHERE RECENT_NOTES.USER_NAME = :USER_NAME\n" +
            "ORDER BY NOTES.ID, JOB.PARAGRAPH_ID, JOB.STARTED_AT DESC;";

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public NoteStatisticsDAO(final NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    }

    private static String mapRow(final ResultSet resultSet, final Map<Long, NoteStatistic> buffer) throws SQLException {

        final Long noteId = resultSet.getLong("NOTE_ID");
        final String noteUUID = resultSet.getString("NOTE_UUID");

        if (!buffer.containsKey(noteId)) {
            buffer.put(noteId, new NoteStatistic(noteId, noteUUID, new ArrayList<>()));
        }

        final LocalDateTime startedAt =
                null != resultSet.getTimestamp("STARTED_AT")
                        ? resultSet.getTimestamp("STARTED_AT").toLocalDateTime()
                        : null;

        final LocalDateTime endedAt =
                null != resultSet.getTimestamp("ENDED_AT")
                        ? resultSet.getTimestamp("ENDED_AT").toLocalDateTime()
                        : null;

        NoteStatisticInner noteStatisticInner = new NoteStatisticInner();
        noteStatisticInner.setParagraphId(resultSet.getLong("PARAGRAPH_ID"));
        noteStatisticInner.setParagraphUuid(resultSet.getString("PARAGRAPH_UUID"));
        noteStatisticInner.setStatus(JobBatch.Status.valueOf(resultSet.getString("STATUS")));
        noteStatisticInner.setUserName(resultSet.getString("USER_NAME"));
        noteStatisticInner.setStartedAt(startedAt);
        noteStatisticInner.setEndedAt(endedAt);
        buffer.get(noteId).getInner().add(noteStatisticInner);

        return StringUtils.EMPTY;
    }

    public Collection<NoteStatistic> getFavouriteNoteStats(final @Nonnull String username) {
        return getNoteStatistics(username, GET_USER_FAV_NOTES_STAT);
    }

    public Collection<NoteStatistic> getRecentNoteStats(final @Nonnull String username) {
        return getNoteStatistics(username, GET_USER_RECENT_NOTES_STAT);
    }

    private Collection<NoteStatistic> getNoteStatistics(@Nonnull String username, String getUserRecentNotesStat) {
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("USER_NAME", username);

        final Map<Long, NoteStatistic> buffer = new HashMap<>();

        namedParameterJdbcTemplate.query(
                getUserRecentNotesStat,
                parameters,
                (rs, i) -> NoteStatisticsDAO.mapRow(rs, buffer));

        return buffer.values();
    }
}

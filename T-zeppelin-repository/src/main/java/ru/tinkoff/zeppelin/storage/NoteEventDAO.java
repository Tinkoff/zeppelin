package ru.tinkoff.zeppelin.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.notebook.NoteEvent;
import ru.tinkoff.zeppelin.core.notebook.NoteSubscription;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Component
public class NoteEventDAO {


    private static final String PERSIST_NOTE_SUBSCRIPTION = "" +
            "INSERT INTO NOTE_EVENT_SUBSCRIBERS (USER_NAME,\n" +
            "                   NOTE_ID,\n" +
            "                   EVENT_TYPE,\n" +
            "                   NOTIFICATION_TYPE,\n" +
            "                   SCHEDULER_ID)\n" +
            "VALUES (:USER_NAME,\n" +
            "        :NOTE_ID,\n" +
            "        :EVENT_TYPE,\n" +
            "        :NOTIFICATION_TYPE,\n" +
            "        :SCHEDULER_ID);";

    private static final String GET_NOTE_EVENT = "" +
            "SELECT NOTIFICATION_TYPE\n" +
            "   , USER_NAME\n" +
            "FROM NOTE_EVENT_SUBSCRIBERS\n" +
            "WHERE NOTE_ID = :NOTE_ID\n" +
            "   AND EVENT_TYPE = :EVENT_TYPE;";

    private static final String DELETE_NOTE_EVENT_SUBSCRIPTION = "" +
            "DELETE\n" +
            "FROM NOTE_EVENT_SUBSCRIBERS\n" +
            "WHERE NOTE_ID = :NOTE_ID\n" +
            "   AND USER_NAME = :USER_NAME\n" +
            "   AND EVENT_TYPE = :EVENT_TYPE\n" +
            "   AND NOTIFICATION_TYPE = :NOTIFICATION_TYPE;";

    private static final String CHECK_NOTE_EVENT_SUBSCRIPTION = "" +
            "SELECT ID\n" +
            "FROM NOTE_EVENT_SUBSCRIBERS\n" +
            "WHERE NOTE_ID = :NOTE_ID\n" +
            "   AND USER_NAME = :USER_NAME\n" +
            "   AND EVENT_TYPE = :EVENT_TYPE\n" +
            "   AND NOTIFICATION_TYPE = :NOTIFICATION_TYPE;";


    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private static final Logger LOG = LoggerFactory.getLogger(NoteEventDAO.class);

    public NoteEventDAO(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    }

    private static NoteSubscription mapRow(final  ResultSet resultSet, final int i) throws SQLException {
        return new NoteSubscription(
                resultSet.getString("USER_NAME"),
                NoteEvent.Notification.valueOf(resultSet.getString("NOTIFICATION_TYPE")));
    }

    private static Long mapRowCheck(final ResultSet resultSet,final  int i) throws SQLException{
        return resultSet.getLong("ID");
    }

    public NoteEvent persist(final NoteEvent noteEvent){
        final KeyHolder holder = new GeneratedKeyHolder();
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("NOTE_ID", noteEvent.getNoteId())
                .addValue("USER_NAME", noteEvent.getUser())
                .addValue("EVENT_TYPE", noteEvent.getType().toString())
                .addValue("NOTIFICATION_TYPE", noteEvent.getNotification().toString())
                .addValue("SCHEDULER_ID", noteEvent.getSchedulerId());
        namedParameterJdbcTemplate.update(PERSIST_NOTE_SUBSCRIPTION, parameters, holder);

        noteEvent.setId((Long) holder.getKeys().get("id"));
        return noteEvent;
    }

    public void remove(final NoteEvent noteEvent){
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("NOTE_ID", noteEvent.getNoteId())
                .addValue("USER_NAME", noteEvent.getUser())
                .addValue("EVENT_TYPE", noteEvent.getType().toString())
                .addValue("NOTIFICATION_TYPE", noteEvent.getNotification().toString());
        namedParameterJdbcTemplate.update(DELETE_NOTE_EVENT_SUBSCRIPTION, parameters);

    }

    public boolean isSubscribed(NoteEvent noteEvent){
        final  SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("NOTE_ID", noteEvent.getNoteId())
                .addValue("USER_NAME", noteEvent.getUser())
                .addValue("EVENT_TYPE", noteEvent.getType().toString())
                .addValue("NOTIFICATION_TYPE", noteEvent.getNotification().toString());
        return !namedParameterJdbcTemplate.query(CHECK_NOTE_EVENT_SUBSCRIPTION, parameters, NoteEventDAO::mapRowCheck).isEmpty();
    }

    public List<NoteSubscription> getNoteEvent(final Long noteId, final String eventType){
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("NOTE_ID", noteId)
                .addValue("EVENT_TYPE", eventType );

        return namedParameterJdbcTemplate.query(
                GET_NOTE_EVENT,
                parameters,
                NoteEventDAO::mapRow);
    }
}

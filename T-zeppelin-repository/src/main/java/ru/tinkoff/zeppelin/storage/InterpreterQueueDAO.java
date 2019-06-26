/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.tinkoff.zeppelin.storage;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.externalDTO.InterpreterQueueDTO;
import ru.tinkoff.zeppelin.core.notebook.JobBatch;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;

@Component
public class InterpreterQueueDAO {

    private static final String SELECT_INTERPRETERS_QUEUES = "" +
            "SELECT DISTINCT ON (J.BATCH_ID)\n" +
            "  J.ID,\n" +
            "  J.NOTE_ID,\n" +
            "  N.UUID AS NOTE_UUID,\n" +
            "  N.PATH AS NOTE_PATH,\n" +
            "  J.PARAGRAPH_ID,\n" +
            "  P.UUID as PARAGRAPH_UUID,\n" +
            "  P.TITLE as PARAGRAPH_TITLE,\n" +
            "  J.INDEX_NUMBER AS PARAGRAPH_POSITION,\n" +
            "  J.SHEBANG,\n" +
            "  J.USER_NAME,\n" +
            "  J.CREATED_AT,\n" +
            "  JB.STATUS\n" +
            "FROM JOB_BATCH JB\n" +
            "LEFT JOIN JOB J \n" +
            "   ON JB.ID = J.BATCH_ID\n" +
            "LEFT JOIN NOTES N \n" +
            "   ON J.NOTE_ID = N.ID\n" +
            "LEFT JOIN PARAGRAPHS P \n" +
            "   ON P.ID = J.PARAGRAPH_ID\n" +
            "WHERE JB.STATUS IN ('PENDING', 'RUNNING')\n" +
            "      AND J.STATUS != 'DONE'" +
            "      AND NOT EXISTS(SELECT * FROM JOB J2 WHERE J2.BATCH_ID = JB.ID AND J2.STATUS =  'ERROR')\n" +
            "ORDER BY J.BATCH_ID, J.PRIORITY DESC, J.INDEX_NUMBER;";

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public InterpreterQueueDAO(final NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    }

    private static InterpreterQueueDTO mapRow(final ResultSet resultSet, final int i) throws SQLException {
        final Long id = resultSet.getLong("ID");
        final Long noteId = resultSet.getLong("NOTE_ID");
        final String noteUuid = resultSet.getString("NOTE_UUID");
        final String notePath = resultSet.getString("NOTE_PATH");
        final Long paragraphId = resultSet.getLong("PARAGRAPH_ID");
        final String paragraphUuid = resultSet.getString("PARAGRAPH_UUID");
        final String paragraphTitle = resultSet.getString("PARAGRAPH_TITLE");
        final Long paragraphPosition = resultSet.getLong("PARAGRAPH_POSITION");
        final String shebang = resultSet.getString("SHEBANG");
        final String username = resultSet.getString("USER_NAME");
        final JobBatch.Status status = JobBatch.Status.valueOf(resultSet.getString("STATUS"));
        final LocalDateTime startedAt =
                resultSet.getTimestamp("CREATED_AT") != null
                        ? resultSet.getTimestamp("CREATED_AT").toLocalDateTime()
                        : null;

        return new InterpreterQueueDTO(id,
                noteId,
                noteUuid,
                notePath,
                paragraphId,
                paragraphUuid,
                paragraphTitle,
                paragraphPosition,
                shebang,
                username,
                startedAt,
                status);
    }

    public Map<String, List<InterpreterQueueDTO>> getInterpreterQueue() {
        final List<InterpreterQueueDTO> jobs = namedParameterJdbcTemplate.query(
                SELECT_INTERPRETERS_QUEUES,
                InterpreterQueueDAO::mapRow);

        final Map<String, List<InterpreterQueueDTO>> interpreterQueue = new HashMap<>();
        jobs.stream()
                .peek(j -> interpreterQueue.putIfAbsent(j.getShebang(), new ArrayList<>()))
                .forEach(j -> interpreterQueue.get(j.getShebang()).add(j));
        interpreterQueue.forEach((key, value) -> value.sort(Comparator.comparing(InterpreterQueueDTO::getParagraphPosition)));
        return interpreterQueue;
    }
}



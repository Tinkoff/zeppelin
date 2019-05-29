package org.apache.zeppelin.rest;


import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.tinkoff.zeppelin.core.notebook.NoteStatistic;
import ru.tinkoff.zeppelin.engine.NoteService;
import ru.tinkoff.zeppelin.storage.NoteStatisticsDAO;
import java.util.*;

@RestController
@RequestMapping("/api/statistics")
public class NotebookStatRestApi extends AbstractRestApi{

    private final NoteStatisticsDAO noteStatisticsDAO;
    private static final Logger LOGGER = LoggerFactory.getLogger(NotebookStatRestApi.class);

    @Autowired
    public NotebookStatRestApi(final NoteService noteService,
                               final ConnectionManager connectionManager, final NoteStatisticsDAO noteStatisticsDAO) {
        super(noteService, connectionManager);
        this.noteStatisticsDAO = noteStatisticsDAO;
    }

    /**
     * Get notes statistics of selected note type  REST API | Endpoint: <b>GET - /api/statistics</b>
     *
     * @param noteType type of selected for statistics notes - favorite/recent
     * @return JSON with status.OK
     */
    @GetMapping(produces = "application/json")
    public ResponseEntity get(@RequestParam("note_type") final String noteType) {
        LOGGER.info("Получение статистики выполнения параграфов избранных/недавних ноутов");

        final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
        final Collection<NoteStatistic> noteStatistics ;

        if (noteType == null || noteType.isEmpty() || !(noteType.equals("favorite") || noteType.equals("recent"))) {
            return new JsonResponse(HttpStatus.BAD_REQUEST, "note_type no correct").build();
        }

        if (noteType.equals("favorite"))
            noteStatistics = noteStatisticsDAO.getFavouriteNoteStats(authenticationInfo.getUser());
        else
            noteStatistics = noteStatisticsDAO.getRecentNoteStats(authenticationInfo.getUser());

        return new JsonResponse(HttpStatus.OK, noteStatistics).build();
    }
}

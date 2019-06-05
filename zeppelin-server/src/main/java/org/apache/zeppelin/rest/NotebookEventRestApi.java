package org.apache.zeppelin.rest;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.tinkoff.zeppelin.core.notebook.NoteEvent;
import ru.tinkoff.zeppelin.engine.NoteService;
import ru.tinkoff.zeppelin.storage.NoteDAO;
import ru.tinkoff.zeppelin.storage.NoteEventDAO;
import ru.tinkoff.zeppelin.storage.SchedulerDAO;

@RestController
@RequestMapping("/api/event")
public class NotebookEventRestApi extends AbstractRestApi {
    private final NoteEventDAO noteEventDAO;
    private final SchedulerDAO schedulerDAO;
    private final NoteDAO noteDAO;

    @Autowired
    public NotebookEventRestApi(final NoteService noteService,
                                final ConnectionManager connectionManager,
                                final NoteEventDAO noteEventDAO,
                                final SchedulerDAO schedulerDAO,
                                final NoteDAO noteDAO) {
        super(noteService, connectionManager);
        this.noteEventDAO = noteEventDAO;
        this.schedulerDAO = schedulerDAO;
        this.noteDAO = noteDAO;
    }


    @PostMapping(value = "/{noteId}", produces = "application/json")
    public ResponseEntity noteSubscription(
            @PathVariable("noteId") final Long noteId,
            @RequestParam("type") final String eventType,
            @RequestParam("action") final String eventAction,
            @RequestParam("notification") final String eventNotification) {

        final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

        if (noteDAO.get(noteId) == null || schedulerDAO.getByNote(noteId) == null){
            return new JsonResponse(HttpStatus.BAD_REQUEST, "Scheduler or note not exist.").build();
        }

        if (StringUtils.isEmpty(eventType) || !NoteEvent.Type.containsName(eventType)) {
            return new JsonResponse(HttpStatus.BAD_REQUEST, "Event type not correct").build();
        }

        if (StringUtils.isEmpty(eventAction) || !NoteEvent.Notification.containsName(eventNotification)) {
            return new JsonResponse(HttpStatus.BAD_REQUEST, "Event action not correct").build();
        }


        final NoteEvent noteEvent = new NoteEvent(authenticationInfo.getUser(),
                noteId,
                NoteEvent.Type.valueOf(eventType.toUpperCase()),
                NoteEvent.Notification.valueOf(eventNotification.toUpperCase()),
                schedulerDAO.getByNote(noteId).getId()
        );

        final boolean isSubscribed = noteEventDAO.isSubscribed(noteEvent);

        if (eventAction.equals("add") && !isSubscribed) {
            noteEventDAO.persist(noteEvent);
        } else if (eventAction.equals("remove")) {
            if (isSubscribed){
                noteEventDAO.remove(noteEvent);
            } else {
                return new JsonResponse(HttpStatus.BAD_REQUEST,
                        "User not subscribed on selected note event with selected notification type")
                        .build();
            }
        }
        return new JsonResponse(HttpStatus.OK).build();
    }
}

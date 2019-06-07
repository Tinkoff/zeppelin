package org.apache.zeppelin.rest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.NoteEvent;
import ru.tinkoff.zeppelin.core.notebook.NoteEvent.Notification;
import ru.tinkoff.zeppelin.core.notebook.NoteEvent.Type;
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

    /**
     * Returns note notification configuration.
     * eventType -> {notificationType -> userlist}.
     *
     * @param noteId
     * @return
     */
    @GetMapping(value = "/{noteId}/subscriptions", produces = "application/json")
    public ResponseEntity getNoteSubscriptions(@PathVariable("noteId") final long noteId) {
        final HashMap<String, HashMap<String, List<String>>> subscriptions = new HashMap<>(Type.values().length);

        for (final Type eventType : NoteEvent.Type.values()) {
            final HashMap<String, List<String>> currentEventNotifications = new HashMap<>(Notification.values().length);

            for (final Notification notificationType : Notification.values()) {
                currentEventNotifications.put(notificationType.name(), new ArrayList<>());
            }
            noteEventDAO.getNoteEvent(noteId, eventType.name())
                .forEach(s -> currentEventNotifications.get(s.getNotification().name()).add(s.getName()));

            subscriptions.put(eventType.name(), currentEventNotifications);
        }

        return new JsonResponse(HttpStatus.OK, subscriptions).build();
    }

    @PostMapping(value = "/{noteId}", produces = "application/json")
    public ResponseEntity noteSubscription(
            @PathVariable("noteId") final Long noteId,
            @RequestParam("type") final String eventType,
            @RequestParam("action") final String eventAction,
            @RequestParam("notification") final String eventNotification,
            @RequestParam("user") final String user) {
        final Note note = noteDAO.get(noteId);

        if (note == null || schedulerDAO.getByNote(noteId) == null){
            return new JsonResponse(HttpStatus.BAD_REQUEST, "Scheduler not exist.").build();
        }

        if (!userHasReaderPermission(note)) {
            return new JsonResponse(HttpStatus.UNAUTHORIZED, "Insufficient privileges, you must be a reader of this note").build();
        }

        if (StringUtils.isEmpty(eventType) || !NoteEvent.Type.containsName(eventType)) {
            return new JsonResponse(HttpStatus.BAD_REQUEST, "Event type not correct").build();
        }

        if (StringUtils.isEmpty(eventNotification) || !NoteEvent.Notification.containsName(eventNotification)) {
            return new JsonResponse(HttpStatus.BAD_REQUEST, "Notification type not correct").build();
        }


        final NoteEvent noteEvent = new NoteEvent(
                user,
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

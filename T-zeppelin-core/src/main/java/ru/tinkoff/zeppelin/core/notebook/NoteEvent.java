package ru.tinkoff.zeppelin.core.notebook;

import java.util.Arrays;

public class NoteEvent {

    public enum Type {
        RUN,
        ERROR,
        SCHEDULE_CHANGE;

        public static boolean containsName(final String value) {
            return Arrays.stream(values())
                    .map(Enum::name)
                    .anyMatch(s -> s.equals(value.toUpperCase()));
        }
    }

    public enum Notification {
        EMAIL;

        public static boolean containsName(final String value) {
            return Arrays.stream(values())
                    .map(Enum::name)
                    .anyMatch(s -> s.equals(value.toUpperCase()));
        }
    }

    private Long id;
    private final String user;
    private final Long noteId;
    private final Type type;
    private final Notification notification;
    private final Long schedulerId;

    public NoteEvent(String user,
                     Long noteId,
                     Type type,
                     Notification notification,
                     Long schedulerId) {
        this.user = user;
        this.noteId = noteId;
        this.type = type;
        this.notification = notification;
        this.schedulerId = schedulerId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public Long getNoteId() {
        return noteId;
    }

    public Type getType() {
        return type;
    }

    public Long getSchedulerId() {
        return schedulerId;
    }

    public Notification getNotification() {
        return notification;
    }

}

package ru.tinkoff.zeppelin.core.notebook;

public class NoteSubscription {
    private final String name;
    private final NoteEvent.Notification notification;

    public NoteSubscription(String name,
                            NoteEvent.Notification notification) {
        this.name = name;
        this.notification = notification;
    }

    public String getName() {
        return name;
    }

    public NoteEvent.Notification getNotification() {
        return notification;
    }
}

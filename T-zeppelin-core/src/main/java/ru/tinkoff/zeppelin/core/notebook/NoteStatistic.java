package ru.tinkoff.zeppelin.core.notebook;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

public class NoteStatistic {


    //private long id;

    private long noteId;
    private String noteUuid;
    private List<NoteStatisticInner> inner;

    public NoteStatistic(long noteId, String noteUuid, List<NoteStatisticInner> inner) {
        this.noteId = noteId;
        this.noteUuid = noteUuid;
        this.inner = inner;
    }

    public long getNoteId() {
        return noteId;
    }

    public String getNoteUuid() {
        return noteUuid;
    }

    public List<NoteStatisticInner> getInner() {
        return inner;
    }
}

package ru.tinkoff.zeppelin.core.notebook;

import java.time.LocalDateTime;

public class NoteStatisticInner {
    private long paragraphId;
    private String paragraphUuid;
    private String userName;
    private JobBatch.Status status;
    private LocalDateTime startedAt;
    private LocalDateTime endedAt;


    public NoteStatisticInner() {
    }

    public String getUserName() { return userName; }

    public void setUserName(String userName) { this.userName = userName; }

    public long getParagraphId() {
        return paragraphId;
    }

    public void setParagraphId(long paragraphId) {
        this.paragraphId = paragraphId;
    }

    public JobBatch.Status getStatus() {
        return status;
    }

    public void setStatus(JobBatch.Status status) {
        this.status = status;
    }

    public LocalDateTime getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(LocalDateTime startedAt) {
        this.startedAt = startedAt;
    }

    public LocalDateTime getEndedAt() {
        return endedAt;
    }

    public void setEndedAt(LocalDateTime endedAt) {
        this.endedAt = endedAt;
    }

    public String getParagraphUuid() {
        return paragraphUuid;
    }

    public void setParagraphUuid(String paragraphUuid) {
        this.paragraphUuid = paragraphUuid;
    }

}

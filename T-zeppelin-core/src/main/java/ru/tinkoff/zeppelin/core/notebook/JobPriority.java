package ru.tinkoff.zeppelin.core.notebook;

public enum JobPriority {
    USER(0),
    SCHEDULER(100);

    private final int index;

    JobPriority(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }
}

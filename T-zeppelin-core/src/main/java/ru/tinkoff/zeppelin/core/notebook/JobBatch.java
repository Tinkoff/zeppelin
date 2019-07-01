/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.tinkoff.zeppelin.core.notebook;

import com.google.common.collect.Sets;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Set;

public class JobBatch {

  public enum Status {

    SAVING,
    PENDING,
    RUNNING,
    DONE,
    ERROR,
    ABORTING,
    ABORTED,
    ;

    public static Set<Status> getRunningStatuses() {
      return Sets.newHashSet(Status.PENDING, Status.RUNNING, Status.ABORTING);
    }
  }

  private long id;
  private long noteId;
  private Status status;
  private LocalDateTime createdAt;
  private LocalDateTime startedAt;
  private LocalDateTime endedAt;


  public long getId() {
    return id;
  }

  public void setId(final long id) {
    this.id = id;
  }

  public long getNoteId() {
    return noteId;
  }

  public void setNoteId(final long noteId) {
    this.noteId = noteId;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(final Status status) {
    this.status = status;
  }

  public LocalDateTime getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(LocalDateTime createdAt) {
    this.createdAt = createdAt;
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

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof JobBatch)) return false;
    final JobBatch jobBatch = (JobBatch) o;
    return id == jobBatch.id &&
        noteId == jobBatch.noteId &&
        status == jobBatch.status &&
        Objects.equals(createdAt, jobBatch.createdAt) &&
        Objects.equals(startedAt, jobBatch.startedAt) &&
        Objects.equals(endedAt, jobBatch.endedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, noteId, status, createdAt, startedAt, endedAt);
  }
}

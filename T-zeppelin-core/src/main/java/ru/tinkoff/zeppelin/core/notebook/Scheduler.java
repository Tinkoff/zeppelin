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

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Set;

/**
 * Paragraph cron configuration.
 */
public class Scheduler implements Serializable {

  private Long id;
  private final Long noteId;

  private boolean isEnabled;
  private String expression;

  private String user;
  private Set<String> roles;
  private LocalDateTime lastExecution;
  private LocalDateTime nextExecution;

  public Scheduler(final Long noteId) {
    this.noteId = noteId;
  }

  public Scheduler(
          final Long id,
          final Long noteId,
          final boolean isEnabled,
          final String expression,
          final String user,
          final Set<String> roles,
          final LocalDateTime lastExecution,
          final LocalDateTime nextExecution) {
    this.id = id;
    this.noteId = noteId;
    this.isEnabled = isEnabled;
    this.expression = expression;
    this.user = user;
    this.roles = roles;
    this.lastExecution = lastExecution;
    this.nextExecution = nextExecution;
  }

  public Scheduler getScheduler(){
    return  new Scheduler(
            getId(),
            getNoteId(),
            isEnabled(),
            getExpression(),
            getUser(),
            getRoles(),
            getLastExecution(),
            getNextExecution());
  }

  public Long getId() {
    return id;
  }

  public void setId(final Long id) {
    this.id = id;
  }

  public Long getNoteId() {
    return noteId;
  }

  public boolean isEnabled() {
    return isEnabled;
  }

  public void setEnabled(final boolean enabled) {
    isEnabled = enabled;
  }

  public String getExpression() {
    return expression;
  }

  public void setExpression(final String expression) {
    this.expression = expression;
  }

  public String getUser() {
    return user;
  }

  public void setUser(final String user) {
    this.user = user;
  }

  public Set<String> getRoles() {
    return roles;
  }

  public void setRoles(final Set<String> roles) {
    this.roles = roles;
  }

  public LocalDateTime getLastExecution() {
    return lastExecution;
  }

  public void setLastExecution(final LocalDateTime lastExecution) {
    this.lastExecution = lastExecution;
  }

  public LocalDateTime getNextExecution() {
    return nextExecution;
  }

  public void setNextExecution(final LocalDateTime nextExecution) {
    this.nextExecution = nextExecution;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof Scheduler)) return false;
    final Scheduler scheduler = (Scheduler) o;
    return isEnabled == scheduler.isEnabled &&
        Objects.equals(id, scheduler.id) &&
        Objects.equals(noteId, scheduler.noteId) &&
        Objects.equals(expression, scheduler.expression) &&
        Objects.equals(user, scheduler.user) &&
        Objects.equals(roles, scheduler.roles) &&
        Objects.equals(lastExecution, scheduler.lastExecution) &&
        Objects.equals(nextExecution, scheduler.nextExecution);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, noteId, isEnabled, expression, user, roles, lastExecution, nextExecution);
  }
}

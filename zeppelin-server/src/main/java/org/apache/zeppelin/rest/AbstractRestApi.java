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
package org.apache.zeppelin.rest;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.rest.exception.ForbiddenException;
import org.apache.zeppelin.rest.exception.NoteNotFoundException;
import org.apache.zeppelin.rest.exception.ParagraphNotFoundException;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.handler.AbstractHandler.Permission;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.engine.NoteService;

abstract class AbstractRestApi {

  protected final NoteService noteService;
  protected final ConnectionManager connectionManager;

  protected AbstractRestApi(
      final NoteService noteService,
      final ConnectionManager connectionManager) {
    this.noteService = noteService;
    this.connectionManager = connectionManager;
  }

  Note secureLoadNote(final String noteUuid, final Permission permission) {
    final Note note = noteService.getNote(noteUuid);
    return secureLoadNote(note.getId(), permission);
  }

  Note secureLoadNote(final long noteId, final Permission permission) {
    final Note note = noteService.getNote(noteId);

    if (note == null) {
      throw new NoteNotFoundException("Can't find note with id " + noteId);
    }

    if (userHasOwnerPermission(note) || userHasAdminPermission()) {
      return note;
    }

    boolean isAllowed = false;
    Set<String> allowed = null;
    switch (permission) {
      case READER:
        isAllowed = userHasReaderPermission(note);
        allowed = note.getReaders();
        break;
      case WRITER:
        isAllowed = userHasWriterPermission(note);
        allowed = note.getWriters();
        break;
      case RUNNER:
        isAllowed = userHasRunnerPermission(note);
        allowed = note.getRunners();
        break;
    }
    if (!isAllowed) {
      final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
      final String errorMsg = "Insufficient privileges to " + permission + " note.\n" +
          "Allowed users or roles: " + allowed + "\n" + "But the user " +
          authenticationInfo.getUser() + " belongs to: " + authenticationInfo.getRoles();
      throw new ForbiddenException(errorMsg);
    }
    return note;
  }

  Paragraph getParagraph(final Note note, final Long paragraphId) {
    return noteService.getParagraphs(note).stream()
        .filter(p -> p.getId().equals(paragraphId))
        .findAny()
        .orElseThrow(() -> new ParagraphNotFoundException("paragraph not found"));
  }

  <T> void updateIfNotNull(final Supplier<T> getter, final Consumer<T> setter) {
    final T requestParam = getter.get();
    if (requestParam != null) {
      setter.accept(requestParam);
    }
  }

  private boolean userHasAdminPermission() {
    final Set<String> admin = new HashSet<>();
    admin.addAll(Configuration.getAdminUsers());
    admin.addAll(Configuration.getAdminGroups());
    return userRolesContains(admin);
  }

  private boolean userHasOwnerPermission(final Note note) {
    return userRolesContains(note.getOwners());
  }

  private boolean userHasWriterPermission(final Note note) {
    return userRolesContains(note.getWriters());
  }

  private boolean userHasRunnerPermission(final Note note) {
    return userRolesContains(note.getRunners());
  }

  boolean userHasReaderPermission(final Note note) {
    return userRolesContains(note.getReaders());
  }

  private static Set<String> getUserAvailableRoles() {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Set<String> userRoles = new HashSet<>();
    userRoles.add(authenticationInfo.getUser());
    userRoles.addAll(authenticationInfo.getRoles());
    return userRoles;
  }

  private boolean userRolesContains(final Set<String> neededRoles) {
    for (final String availableRole : getUserAvailableRoles()) {
      if (neededRoles.contains(availableRole)) {
        return true;
      }
    }
    return false;
  }
}
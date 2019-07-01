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

package org.apache.zeppelin.websocket.handler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.rest.exception.ForbiddenException;
import org.apache.zeppelin.rest.exception.NoteNotFoundException;
import org.apache.zeppelin.rest.exception.ParagraphNotFoundException;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.SockMessage;
import org.springframework.web.socket.WebSocketSession;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.engine.NoteService;

public abstract class AbstractHandler {

  protected static Gson gson = new GsonBuilder()
          .setDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
          .setPrettyPrinting().create();

  protected final ConnectionManager connectionManager;
  protected final NoteService noteService;

  public AbstractHandler(final ConnectionManager connectionManager, final NoteService noteService) {
    this.connectionManager = connectionManager;
    this.noteService = noteService;
  }

  protected Note safeLoadNote(final String paramName,
                              final SockMessage message,
                              final Permission permission,
                              final AuthenticationInfo authenticationInfo,
                              final WebSocketSession conn) {
    final Long noteId = connectionManager.getAssociatedNoteId(conn) != null
            ? connectionManager.getAssociatedNoteId(conn)
            : noteService.getNote((String)message.getNotNull(paramName)).getId();

    checkPermission(noteId, permission, authenticationInfo);
    final Note note = noteService.getNote(noteId);
    if (note == null) {
      throw new NoteNotFoundException("Can't find note with id '" + noteId +"'.");
    }
    return note;
  }

  protected Paragraph safeLoadParagraph(final String paramName,
                                        final SockMessage fromSockMessage,
                                        final Note note) {
    final String paragraphId = fromSockMessage.getNotNull(paramName);
    final List<Paragraph> paragraphs = noteService.getParagraphs(note);
    final Paragraph p = paragraphs
            .stream()
            .filter(paragraph -> paragraph.getUuid().equals(paragraphId))
            .findFirst()
            .orElse(null);

    if (p == null) {
      throw new ParagraphNotFoundException(paragraphId);
    }
    return p;
  }


  public enum Permission {
    READER,
    WRITER,
    RUNNER,
    OWNER,
    ANY
  }

  protected void checkPermission(final Long noteId,
                                 final Permission permission,
                                 final AuthenticationInfo authenticationInfo) {
    Note target = noteService.getNote(noteId);
    if (permission == Permission.ANY || target == null) {
      return;
    }

    final Set<String> admin = new HashSet<>();
    admin.addAll(Configuration.getAdminUsers());
    admin.addAll(Configuration.getAdminGroups());

    final Set<String> userRoles = new HashSet<>();
    userRoles.addAll(authenticationInfo.getRoles());
    userRoles.add(authenticationInfo.getUser());

    if (userRoles.removeAll(target.getOwners()) || userRoles.removeAll(admin)) {
      // user is owner or admin.
      return;
    }

    boolean isAllowed = false;
    Set<String> allowed = null;
    switch (permission) {
      case READER:
        isAllowed = userRoles.removeAll(target.getReaders());
        allowed = target.getReaders();
        break;
      case WRITER:
        isAllowed = userRoles.removeAll(target.getWriters());
        allowed = target.getWriters();
        break;
      case RUNNER:
        isAllowed = userRoles.removeAll(target.getRunners());
        allowed = target.getRunners();
        break;
    }
    if (!isAllowed) {
      final String errorMsg = "Insufficient privileges to " + permission + " note.\n" +
              "Allowed users or roles: " + allowed + "\n" + "But the user " +
              authenticationInfo.getUser() + " belongs to: " + authenticationInfo.getRoles();
      throw new ForbiddenException(errorMsg);
    }
  }

}

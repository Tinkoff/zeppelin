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

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.apache.zeppelin.websocket.dto.NoteDTOConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;
import ru.tinkoff.zeppelin.core.externalDTO.NoteDTO;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.NoteInfo;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.core.notebook.Scheduler;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.engine.NoteService;
import ru.tinkoff.zeppelin.storage.SchedulerDAO;


@Component
public class NoteHandler extends AbstractHandler {

  private static final String TRASH_FOLDER = "~Trash";

  private final NoteDTOConverter noteDTOConverter;
  private final SchedulerDAO schedulerDAO;

  @Autowired
  public NoteHandler(final NoteService noteService,
      final ConnectionManager connectionManager,
      final NoteDTOConverter noteDTOConverter,
      final SchedulerDAO schedulerDAO) {
    super(connectionManager, noteService);
    this.noteDTOConverter = noteDTOConverter;
    this.schedulerDAO = schedulerDAO;
  }


  public void sendListNotesInfo(final WebSocketSession conn) throws IOException {
    final List<NoteInfo> notesInfo = noteService.getAllNotes().stream()
        .filter(this::userHasOwnerPermission)
        .map(NoteInfo::new)
        .collect(Collectors.toList());

    conn.sendMessage(new SockMessage(Operation.NOTES_INFO).put("notes", notesInfo).toSend());
  }

  public void getHomeNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final String noteId = Configuration.getHomeNodeId();

    checkPermission(0L, Permission.READER, authenticationInfo);
    final Note note = noteService.getNote(noteId);
    if (note != null) {
      connectionManager.addSubscriberToNode(note.getId(), conn);
      conn.sendMessage(new SockMessage(Operation.NOTE).put("note", note).toSend());
    } else {
      connectionManager.removeSubscribersFromAllNote(conn);
      conn.sendMessage(new SockMessage(Operation.NOTE).put("note", null).toSend());
    }
  }

  public void getNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Note note = safeLoadNote("id", fromMessage, Permission.READER, authenticationInfo, conn);
    connectionManager.addSubscriberToNode(note.getId(), conn);
    final NoteDTO noteDTO = noteDTOConverter.convertNoteToDTO(note);
    conn.sendMessage(new SockMessage(Operation.NOTE).put("note", noteDTO).toSend());
  }

  public void updateNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Note note = safeLoadNote("id", fromMessage, Permission.READER, authenticationInfo, conn);
    final String path = normalizePath(fromMessage.getNotNull("path"));

    note.setPath(path);
    noteService.updateNote(note);
    sendListNotesInfo(conn);
  }

  public void deleteNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Note note = noteService.getNote((String) fromMessage.getNotNull("id"));
    if (!userHasOwnerPermission(note)) {
      throw new AccessDeniedException(
          "User " + authenticationInfo.getUser() +
              " is no owner note " + note.getId()
      );
    }
    noteService.deleteNote(note);
    connectionManager.removeNoteSubscribers(note.getId());
    sendListNotesInfo(conn);
  }

  public void createNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    String notePath = normalizePath(fromMessage.getNotNull("path"));

    try {
      final Note note = new Note(notePath);
      note.getReaders().add(authenticationInfo.getUser());
      note.getRunners().add(authenticationInfo.getUser());
      note.getWriters().add(authenticationInfo.getUser());
      note.getOwners().add(authenticationInfo.getUser());
      noteService.persistNote(note);

      // it's an empty note. so add one paragraph
      final Paragraph paragraph = new Paragraph();
      paragraph.setId(null);
      paragraph.setNoteId(note.getId());
      paragraph.setTitle(StringUtils.EMPTY);
      paragraph.setText(StringUtils.EMPTY);
      paragraph.setShebang(null);
      paragraph.setCreated(LocalDateTime.now());
      paragraph.setUpdated(LocalDateTime.now());
      paragraph.setPosition(0);
      paragraph.setJobId(null);
      noteService.persistParagraph(note, paragraph);

      connectionManager.addSubscriberToNode(note.getId(), conn);
      conn.sendMessage(new SockMessage(Operation.NEW_NOTE).put("note", note).toSend());
      sendListNotesInfo(conn);
    } catch (final Exception e) {
      throw new IllegalStateException("Failed to create note.", e);
    }
  }

  public void cloneNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Note note = safeLoadNote("id", fromMessage, Permission.READER, authenticationInfo, conn);
    String path = normalizePath(fromMessage.getNotNull("name"));

    Note cloneNote = new Note(path);
    cloneNote.setPath(path);
    cloneNote.setScheduler(note.getScheduler());
    cloneNote.getReaders().clear();
    cloneNote.getRunners().clear();
    cloneNote.getWriters().clear();
    cloneNote.getOwners().clear();
    cloneNote.getReaders().add(authenticationInfo.getUser());
    cloneNote.getRunners().add(authenticationInfo.getUser());
    cloneNote.getWriters().add(authenticationInfo.getUser());
    cloneNote.getOwners().add(authenticationInfo.getUser());
    cloneNote = noteService.persistNote(cloneNote);

    final List<Paragraph> paragraphs = noteService.getParagraphs(note);
    for (final Paragraph paragraph : paragraphs) {
      final Paragraph cloneParagraph = new Paragraph();
      cloneParagraph.setId(null);
      cloneParagraph.setNoteId(cloneNote.getId());
      cloneParagraph.setTitle(paragraph.getTitle());
      cloneParagraph.setText(paragraph.getText());
      cloneParagraph.setShebang(paragraph.getShebang());
      cloneParagraph.setCreated(LocalDateTime.now());
      cloneParagraph.setUpdated(LocalDateTime.now());
      cloneParagraph.setPosition(paragraph.getPosition());
      cloneParagraph.setJobId(null);
      cloneParagraph.getConfig().putAll(paragraph.getConfig());
      cloneParagraph.getFormParams().putAll(paragraph.getFormParams());
      noteService.persistParagraph(cloneNote, cloneParagraph);
    }

    conn.sendMessage(new SockMessage(Operation.NEW_NOTE).put("note", cloneNote).toSend());
    sendListNotesInfo(conn);
  }


  public void moveNoteToTrash(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Note note = safeLoadNote("id", fromMessage, Permission.OWNER, authenticationInfo, conn);
    note.setPath("/" + TRASH_FOLDER + normalizePath(note.getPath()));
    noteService.updateNote(note);

    //disable scheduler
    Scheduler scheduler = schedulerDAO.getByNote(note.getId());
    if (scheduler != null) {
      scheduler.setEnabled(false);
      schedulerDAO.update(scheduler);
    }

    sendListNotesInfo(conn);
  }

  public void restoreNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Note note = safeLoadNote("id", fromMessage, Permission.OWNER, authenticationInfo, conn);

    if (!note.getPath().startsWith("/" + TRASH_FOLDER)) {
      throw new IOException("Can not restore this note " + note.getPath() + " as it is not in trash folder");
    }

    final String destNotePath = note.getPath().replace("/" + TRASH_FOLDER, "");
    note.setPath(normalizePath(destNotePath));
    noteService.updateNote(note);
    sendListNotesInfo(conn);
  }

  public void restoreFolder(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final String folderPath = normalizePath(fromMessage.getNotNull("id")) + "/";

    if (!folderPath.startsWith("/" + TRASH_FOLDER)) {
      throw new IOException("Can't restore folder: '" + folderPath + "' as it is not in trash folder");
    }

    noteService.getAllNotes().stream()
        .filter(this::userHasOwnerPermission)
        .filter(note -> note.getPath().startsWith(folderPath))
        .forEach(note -> {
          String notePath = normalizePath(note.getPath().substring(TRASH_FOLDER.length() + 1));
          note.setPath(notePath);
          noteService.updateNote(note);
        });
    sendListNotesInfo(conn);
  }


  public void renameFolder(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final String oldFolderPath = normalizePath(fromMessage.getNotNull("id")) + "/";
    final String newFolderPath = normalizePath(fromMessage.getNotNull("name")) + "/";

    noteService.getAllNotes().stream()
        .filter(this::userHasOwnerPermission)
        .filter(note -> note.getPath().startsWith(oldFolderPath))
        .forEach(note -> {
          String notePath =
              normalizePath(note.getPath().replaceFirst(oldFolderPath, newFolderPath));
          note.setPath(notePath);
          noteService.updateNote(note);
        });
    sendListNotesInfo(conn);
  }

  public void moveFolderToTrash(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final String folderPath = normalizePath(fromMessage.getNotNull("id")) + "/";

    noteService.getAllNotes().stream()
        .filter(note -> note.getPath().startsWith(folderPath))
        .filter(this::userHasOwnerPermission)
        .forEach(note -> {
          String notePath = "/" + TRASH_FOLDER + normalizePath(note.getPath());
          note.setPath(notePath);
          noteService.updateNote(note);
        });
    sendListNotesInfo(conn);
  }

  public void removeFolder(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final String folderPath = normalizePath(fromMessage.getNotNull("id")) + "/";

    noteService.getAllNotes().stream()
        .filter(this::userHasOwnerPermission)
        .filter(note -> note.getPath().startsWith(folderPath))
        .forEach(noteService::deleteNote);
    sendListNotesInfo(conn);
  }

  public void emptyTrash(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    noteService.getAllNotes().stream()
        .filter(note -> note.getPath().startsWith("/" + TRASH_FOLDER + "/"))
        .filter(this::userHasOwnerPermission)
        .forEach(noteService::deleteNote);
    sendListNotesInfo(conn);
  }

  public void restoreAll(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    noteService.getAllNotes().stream()
        .filter(this::userHasOwnerPermission)
        .filter(note -> note.getPath().startsWith("/" + TRASH_FOLDER + "/"))
        .forEach(note -> {
          String notePath = normalizePath(note.getPath().substring(TRASH_FOLDER.length() + 1));
          note.setPath(notePath);
          noteService.updateNote(note);
        });
    sendListNotesInfo(conn);
  }

  public void clearAllParagraphOutput(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Note note = safeLoadNote("id", fromMessage, Permission.WRITER, authenticationInfo, conn);

    for (final Paragraph paragraph : noteService.getParagraphs(note)) {
      paragraph.setJobId(null);
      noteService.updateParagraph(note, paragraph);
    }
  }

  private boolean userHasOwnerPermission(final Note note) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Set<String> admin = new HashSet<>();
    admin.addAll(Configuration.getAdminUsers());
    admin.addAll(Configuration.getAdminGroups());

    final Set<String> userRoles = new HashSet<>();
    userRoles.addAll(authenticationInfo.getRoles());
    userRoles.add(authenticationInfo.getUser());

    return userRoles.removeAll(admin) || userRoles.removeAll(note.getOwners());
  }

  private static String normalizePath(String path) {
    // fix 'folder/noteName' --> '/folder/noteName'
    if (!path.startsWith("/")) {
      path = "/" + path;
    }

    // fix '///folder//noteName' --> '/folder/noteName'
    while (path.contains("//")) {
      path = path.replaceAll("//", "/");
    }

    //fix '/folder/noteName/' --> '/folder/noteName'
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    return path;
  }
}

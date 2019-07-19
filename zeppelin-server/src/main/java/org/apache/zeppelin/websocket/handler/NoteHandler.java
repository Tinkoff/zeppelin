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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.apache.zeppelin.websocket.dto.NoteDTOConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;
import ru.tinkoff.zeppelin.core.externalDTO.NoteDTO;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Note.NoteViewMode;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.core.notebook.Scheduler;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.engine.NoteEventService;
import ru.tinkoff.zeppelin.engine.NoteService;
import ru.tinkoff.zeppelin.storage.SchedulerDAO;
import ru.tinkoff.zeppelin.storage.SystemEventType.ET;
import ru.tinkoff.zeppelin.storage.ZLog;


@Component
public class NoteHandler extends AbstractHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(NoteHandler.class);

  private final NoteDTOConverter noteDTOConverter;
  private final SchedulerDAO schedulerDAO;
  private final NoteEventService noteEventService;

  @Autowired
  public NoteHandler(final NoteService noteService,
      final ConnectionManager connectionManager,
      final NoteDTOConverter noteDTOConverter,
      final SchedulerDAO schedulerDAO,
      final NoteEventService noteEventService) {
    super(connectionManager, noteService);
    this.noteDTOConverter = noteDTOConverter;
    this.schedulerDAO = schedulerDAO;
    this.noteEventService = noteEventService;
  }

  public static class NoteInfo {
    String id;
    String path;

    public NoteInfo(final Note note) {
      id = note.getUuid();
      path = note.getPath();
    }
  }
  public void sendListNotesInfo(final WebSocketSession conn) throws IOException {
    final List<NoteInfo> notesInfo = noteService.getAllNotes().stream()
        .filter(this::userHasReaderPermission)
        .map(NoteInfo::new)
        .collect(Collectors.toList());
    LOGGER.info("Загрузка списка ноутов");
    conn.sendMessage(new SockMessage(Operation.NOTES_INFO).put("notes", notesInfo).toSend());
  }

  public void getHomeNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final String noteId = Configuration.getHomeNodeId();
    checkPermission(0L, Permission.READER, authenticationInfo);
    final Note note = noteService.getNote(noteId);
    if (note != null) {
      connectionManager.addSubscriberToNode(note.getId(), conn);
      LOGGER.info("Загрузка ноута по умолчанию noteId: {}, noteUuid: {}", note.getId(), note.getUuid());
      conn.sendMessage(new SockMessage(Operation.NOTE).put("note", note).toSend());
    } else {
      LOGGER.info("Загрузка стартовой страницы");
      connectionManager.removeSubscribersFromAllNote(conn);
      conn.sendMessage(new SockMessage(Operation.NOTE).put("note", null).toSend());
    }
  }

  public void getNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Note note = safeLoadNote("id", fromMessage, Permission.READER, authenticationInfo, conn);
    LOGGER.info("Загрузка ноута noteId: {}, noteUuid: {}", note.getId(), note.getUuid());

    connectionManager.addSubscriberToNode(note.getId(), conn);
    final NoteDTO noteDTO = noteDTOConverter.convertNoteToDTO(note);
    conn.sendMessage(new SockMessage(Operation.NOTE).put("note", noteDTO).toSend());
    ZLog.log(ET.NOTE_OPENED, String.format("Пользователь открыл ноут \"%s\"", note.getUuid()), authenticationInfo.getUser());
  }

  public void updateNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Note note = safeLoadNote("id", fromMessage, Permission.READER, authenticationInfo, conn);
    final String path = normalizePath(fromMessage.getNotNull("path"));
    final NoteViewMode viewMode = NoteViewMode.valueOf(fromMessage.getOrDefault("mode", note.getViewMode().name()));
    LOGGER.info("Обновление ноута noteId: {}, noteUuid: {}, viewMode: {}", note.getId(), note.getUuid(), note.getViewMode());
    note.setPath(path);
    note.setViewMode(viewMode);
    noteService.updateNote(note);
    sendListNotesInfo(conn);
  }

  public void deleteNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Note note = noteService.getNote((String) fromMessage.getNotNull("id"));
    LOGGER.info("Удаление ноута noteId: {}, noteUuid: {}", note.getId(), note.getUuid());

    if (!userHasOwnerPermission(note)) {
      throw new AccessDeniedException(
          "User " + authenticationInfo.getUser() +
              " is no owner note " + note.getId()
      );
    }
    noteService.deleteNote(note);
    connectionManager.removeNoteSubscribers(note.getId());
    sendListNotesInfo(conn);
    ZLog.log(ET.NOTE_DELETED, String.format("Пользователь удалил ноут \"%s\"", note.getUuid()), authenticationInfo.getUser());
  }

  public void createNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final String notePath = normalizePath(fromMessage.getNotNull("path"));

    try {
      final Note note = new Note(notePath);
      addPermissionsToNote(note);
      noteService.persistNote(note);

      LOGGER.info("Создание ноута noteId: {}, noteUuid: {}", note.getId(), note.getUuid());
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
      ZLog.log(ET.NOTE_CREATED, String.format("Пользователь успешно создал ноут \"%s\"", note.getUuid()), authenticationInfo.getUser());
    } catch (final Exception e) {
      ZLog.log(ET.FAILED_TO_CREATE_NOTE, String.format("Ошибка при создании ноутбука: %s",
          ExceptionUtils.getStackTrace(e)), authenticationInfo.getUser());
      throw new IllegalStateException("Failed to create note.", e);
    }
  }

  public void cloneNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Note note = safeLoadNote("id", fromMessage, Permission.READER, authenticationInfo, conn);
    final String path = normalizePath(fromMessage.getNotNull("name"));

    Note cloneNote = new Note(path);
    cloneNote.setPath(path);
    cloneNote.getReaders().clear();
    cloneNote.getRunners().clear();
    cloneNote.getWriters().clear();
    cloneNote.getOwners().clear();
    addPermissionsToNote(cloneNote);

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

    connectionManager.removeNoteSubscribers(note.getId());

    LOGGER.info("Клонирование ноута noteId: {}, noteUuid: {} (новый ноут noteId: {}, noteUuid: {})",
        note.getId(), note.getUuid(), cloneNote.getId(), cloneNote.getUuid());
    conn.sendMessage(new SockMessage(Operation.NEW_NOTE).put("note", cloneNote).toSend());
    sendListNotesInfo(conn);
  }

  @SuppressWarnings("Duplicates")
  private void addPermissionsToNote(final Note note) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Map<Set<String>, Set<String>> actualAndDefaultPermissionMap = new IdentityHashMap<>();
    actualAndDefaultPermissionMap.put(note.getOwners(), Configuration.getDefaultOwners());
    actualAndDefaultPermissionMap.put(note.getWriters(), Configuration.getDefaultWriters());
    actualAndDefaultPermissionMap.put(note.getRunners(), Configuration.getDefaultRunners());
    actualAndDefaultPermissionMap.put(note.getReaders(), Configuration.getDefaultReaders());

    for (final Map.Entry<Set<String>, Set<String>> permEntry : actualAndDefaultPermissionMap.entrySet()) {
      final Set<String> noteActualPerm = permEntry.getKey();
      final Set<String> permToAdd = permEntry.getValue();
      for (String s : permToAdd) {
        s = s.trim().toLowerCase();
        switch (s) {
          case "{username}":
            noteActualPerm.add(authenticationInfo.getUser());
            break;
          case "{usergroups}":
            noteActualPerm.addAll(authenticationInfo.getRoles());
            break;
          default:
            noteActualPerm.add(s);
        }
      }
    }
  }


  public void moveNoteToTrash(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Note note = safeLoadNote("id", fromMessage, Permission.OWNER, authenticationInfo, conn);
    LOGGER.info("Перемещение в корзину ноута noteId: {}, noteUuid: {}", note.getId(), note.getUuid());

    note.setPath("/" + Note.TRASH_FOLDER + normalizePath(note.getPath()));
    noteService.updateNote(note);

    //disable scheduler
    final Scheduler scheduler = schedulerDAO.getByNote(note.getId());
    if (scheduler != null) {
      final Scheduler oldScheduler = scheduler.getScheduler();
      scheduler.setEnabled(false);
      schedulerDAO.update(scheduler);
      noteEventService.noteScheduleChange(note, oldScheduler);
    }
    sendListNotesInfo(conn);
  }

  public void restoreNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Note note = safeLoadNote("id", fromMessage, Permission.OWNER, authenticationInfo, conn);
    LOGGER.info("Восттановление из корзины ноута noteId: {}, noteUuid: {}", note.getId(), note.getUuid());

    if (!note.getPath().startsWith("/" + Note.TRASH_FOLDER)) {
      throw new IOException("Can not restore this note " + note.getPath() + " as it is not in trash folder");
    }

    final String destNotePath = note.getPath().replace("/" + Note.TRASH_FOLDER, "");
    note.setPath(normalizePath(destNotePath));
    noteService.updateNote(note);
    sendListNotesInfo(conn);
    ZLog.log(ET.NOTE_RESTORED, String.format("Пользователь восстановил ноут \"%s\"из корзины",
        note.getUuid()), authenticationInfo.getUser());
  }

  public void restoreFolder(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final String folderPath = normalizePath(fromMessage.getNotNull("id")) + "/";
    final String user = AuthorizationService.getAuthenticationInfo().getUser();
    LOGGER.info("Восcтановление папки {}", folderPath);

    if (!folderPath.startsWith("/" + Note.TRASH_FOLDER)) {
      throw new IOException("Can't restore folder: '" + folderPath + "' as it is not in trash folder");
    }

    noteService.getAllNotes().stream()
        .filter(this::userHasOwnerPermission)
        .filter(note -> note.getPath().startsWith(folderPath))
        .forEach(note -> {
          final String notePath = normalizePath(note.getPath().substring(Note.TRASH_FOLDER.length() + 1));
          note.setPath(notePath);
          noteService.updateNote(note);
          ZLog.log(ET.NOTE_RESTORED, String.format("Пользователь восстановил ноут \"%s\" из корзины",
              note.getUuid()), user);
        });
    sendListNotesInfo(conn);
  }


  public void renameFolder(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final String oldFolderPath = normalizePath(fromMessage.getNotNull("id")) + "/";
    final String newFolderPath = normalizePath(fromMessage.getNotNull("name")) + "/";
    LOGGER.info("Переименование папки {} в {}", oldFolderPath, newFolderPath);

    noteService.getAllNotes().stream()
        .filter(this::userHasOwnerPermission)
        .filter(note -> note.getPath().startsWith(oldFolderPath))
        .forEach(note -> {
          final String notePath =
              normalizePath(note.getPath().replaceFirst(oldFolderPath, newFolderPath));
          note.setPath(notePath);
          noteService.updateNote(note);
        });
    sendListNotesInfo(conn);
  }

  public void moveFolderToTrash(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final String folderPath = normalizePath(fromMessage.getNotNull("id")) + "/";
    LOGGER.info("Перемещение папки {} в корзину", folderPath);
    noteService.getAllNotes().stream()
        .filter(note -> note.getPath().startsWith(folderPath))
        .filter(this::userHasOwnerPermission)
        .forEach(note -> {
          final String notePath = "/" + Note.TRASH_FOLDER + normalizePath(note.getPath());
          note.setPath(notePath);
          noteService.updateNote(note);
        });
    sendListNotesInfo(conn);
  }

  public void removeFolder(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final String folderPath = normalizePath(fromMessage.getNotNull("id")) + "/";
    final String user = AuthorizationService.getAuthenticationInfo().getUser();

    LOGGER.info("Удаление папки {}", folderPath);
    noteService.getAllNotes().stream()
        .filter(this::userHasOwnerPermission)
        .filter(note -> note.getPath().startsWith(folderPath))
        .forEach(n -> {
          noteService.deleteNote(n);
          ZLog.log(ET.NOTE_DELETED, String.format("Пользователь удалил ноут \"%s\"", n.getUuid()), user);
        });
    sendListNotesInfo(conn);
  }

  public void emptyTrash(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    LOGGER.info("Удаление всех объектов из корзины");
    final String user = AuthorizationService.getAuthenticationInfo().getUser();

    noteService.getAllNotes().stream()
        .filter(note -> note.getPath().startsWith("/" + Note.TRASH_FOLDER + "/"))
        .filter(this::userHasOwnerPermission)
        .forEach(n -> {
          noteService.deleteNote(n);
          ZLog.log(ET.NOTE_DELETED, String.format("Пользователь удалил ноут \"%s\"", n.getUuid()), user);
        });
    sendListNotesInfo(conn);
  }

  public void restoreAll(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    LOGGER.info("Восстановление всех объектов из корзины");
    noteService.getAllNotes().stream()
        .filter(this::userHasOwnerPermission)
        .filter(note -> note.getPath().startsWith("/" + Note.TRASH_FOLDER + "/"))
        .forEach(note -> {
          final String notePath = normalizePath(note.getPath().substring(Note.TRASH_FOLDER.length() + 1));
          note.setPath(notePath);
          noteService.updateNote(note);
        });
    sendListNotesInfo(conn);
  }

  public void clearAllParagraphOutput(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Note note = safeLoadNote("id", fromMessage, Permission.WRITER, authenticationInfo, conn);
    LOGGER.info("Очистка вывода результатов для всех параграфов ноута noteId: {}, noteUuid: {}", note.getId(), note.getUuid());

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

  private boolean userHasReaderPermission(final Note note) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Set<String> admin = new HashSet<>();
    admin.addAll(Configuration.getAdminUsers());
    admin.addAll(Configuration.getAdminGroups());

    final Set<String> userRoles = new HashSet<>();
    userRoles.addAll(authenticationInfo.getRoles());
    userRoles.add(authenticationInfo.getUser());

    return userRoles.removeAll(admin) || userRoles.removeAll(note.getReaders()) || userRoles.removeAll(note.getOwners());
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

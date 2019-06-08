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
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.rest.exception.BadRequestException;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.bitbucket.cowwoc.diffmatchpatch.DiffMatchPatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.NoteService;

@Component
public class ParagraphHandler extends AbstractHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParagraphHandler.class);

  @Autowired
  public ParagraphHandler(final NoteService noteService,
                          final ConnectionManager connectionManager) {
    super(connectionManager, noteService);
  }

  public void updateParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, authenticationInfo, conn);
    final Paragraph paragraph = safeLoadParagraph("id", fromMessage, note);

    LOGGER.info("Обновление параграфа noteId: {}, noteUuid : {} paragraphId: {}",
        note.getId(), note.getUuid(), paragraph.getId());

    //final ParagraphDTO before = fullParagraphDAO.getById(paragraph.getId());

    final String title = fromMessage.getNotNull("title");
    final String shebang = fromMessage.getNotNull("shebang");
    final String text = fromMessage.getNotNull("paragraph");
    final Map<String, Object> config = fromMessage.getNotNull("config");


    final DiffMatchPatch dmp = new DiffMatchPatch();
    LinkedList<DiffMatchPatch.Patch> patches = null;
    try {
      patches = dmp.patchMake(paragraph.getText(), text);
    } catch (ClassCastException e) {
      LOGGER.error("Failed to parse patches", e);
    }

    final String paragraphText = paragraph.getText() == null
            ? StringUtils.EMPTY
            : paragraph.getText();

    paragraph.getConfig().putAll(config);
    paragraph.setTitle(title);
    paragraph.setText(
            patches != null
                    ? (String) dmp.patchApply(patches, paragraphText)[0]
                    : text);
    paragraph.setShebang(shebang);

    noteService.updateParagraph(note, paragraph);
  }

  public void removeParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, authenticationInfo, conn);
    final Paragraph p = safeLoadParagraph("id", fromMessage, note);

    LOGGER.info("Удаление параграфа noteId: {}, noteUuid : {} paragraphId: {} ",
        note.getId(), note.getUuid(), p.getId());
    noteService.removeParagraph(note, p);
    final List<Paragraph> paragraphs = noteService.getParagraphs(note);
    paragraphs.sort(Comparator.comparingInt(Paragraph::getPosition));

    for (int i = 0; i < paragraphs.size(); i++) {
      final Paragraph paragraph = paragraphs.get(i);
      if (paragraph.getPosition() == i) {
        continue;
      }
      paragraph.setPosition(i);
      noteService.updateParagraph(note, paragraph);
    }
  }

  public void clearParagraphOutput(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, authenticationInfo, conn);
    final Paragraph p = safeLoadParagraph("id", fromMessage, note);

    LOGGER.info("Очистка результата выполнения параграфа noteId: {}, noteUuid: {} paragraphId: {}",
        note.getId(), note.getUuid(), p.getId());
    p.setJobId(null);
    noteService.updateParagraph(note, p);
  }

  public void moveParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, authenticationInfo, conn);
    final Paragraph paragraphFrom = safeLoadParagraph("id", fromMessage, note);
    final int indexFrom = paragraphFrom.getPosition();
    final int indexTo = ((Double) fromMessage.getNotNull("index")).intValue();

    LOGGER.info("Перемещение параграфа noteId: {}, noteUuid: {} paragraphId: {} ",
        note.getId(), note.getUuid(), paragraphFrom.getId());
    final List<Paragraph> paragraphs = noteService.getParagraphs(note);
    if (indexTo < 0 || indexTo > paragraphs.size()) {
      throw new BadRequestException("newIndex " + indexTo + " is out of bounds");
    }

    Collections.swap(paragraphs, indexFrom, indexTo);

    for (int i = 0; i < paragraphs.size(); i++) {
      final Paragraph paragraph = paragraphs.get(i);
      paragraph.setPosition(i);
      noteService.updateParagraph(note, paragraph);
    }

    connectionManager.broadcast(
        note.getId(),
        new SockMessage(Operation.PARAGRAPH_MOVED)
            .put("id", paragraphFrom.getUuid())
            .put("index", indexTo)
    );
  }

  public String insertParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, authenticationInfo, conn);
    final int index = ((Double) fromMessage.getNotNull("index")).intValue();

    final List<Paragraph> paragraphs = noteService.getParagraphs(note);
    if (index < 0 || index > paragraphs.size()) {
      throw new BadRequestException("newIndex " + index + " is out of bounds");
    }

    for (int i = index; i < paragraphs.size(); i++) {
      final Paragraph p = paragraphs.get(i);
      p.setPosition(i + 1);
      noteService.updateParagraph(note, p);
    }

    final Paragraph paragraph = new Paragraph();
    paragraph.setId(null);
    paragraph.setNoteId(note.getId());
    paragraph.setTitle(StringUtils.EMPTY);
    paragraph.setText(StringUtils.EMPTY);
    paragraph.setShebang(null);
    paragraph.setCreated(LocalDateTime.now());
    paragraph.setUpdated(LocalDateTime.now());
    paragraph.setPosition(index);
    paragraph.setJobId(null);
    noteService.persistParagraph(note, paragraph);

    LOGGER.info("Добавление параграфа noteId: {}, noteUuid: {} paragraphId: {} ",
        note.getId(), note.getUuid(), paragraph.getId());
    return paragraph.getUuid();
  }

  public void copyParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final String paragraphId = insertParagraph(conn, fromMessage);

    if (paragraphId == null) {
      throw new BadRequestException("paragraphId is not defined");
    }
    fromMessage.put("id", paragraphId);
    LOGGER.info("Копирование параграфа paragraphId: {}, сообщение: {}", paragraphId, fromMessage);
    updateParagraph(conn, fromMessage);
  }
}

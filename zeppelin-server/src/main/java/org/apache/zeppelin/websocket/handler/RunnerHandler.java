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

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.NoteExecutorService;
import ru.tinkoff.zeppelin.engine.NoteService;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class RunnerHandler extends AbstractHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RunnerHandler.class);

  private final NoteExecutorService noteExecutorService;


  @Autowired
  public RunnerHandler(final ConnectionManager connectionManager,
                       final NoteService noteService,
                       final NoteExecutorService noteExecutorService) {
    super(connectionManager, noteService);
    this.noteExecutorService = noteExecutorService;
  }


  public void stopNoteExecution(final WebSocketSession conn,
                                final SockMessage fromMessage) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Note note = safeLoadNote("noteId", fromMessage, Permission.RUNNER, authenticationInfo, conn);
    LOGGER.info("Отановка выполнения ноута noteId: {},  noteUuid: " + note.getUuid(), note.getId());
    noteExecutorService.abort(note);
  }

  public void runAllParagraphs(final WebSocketSession conn, final SockMessage fromMessage) {

    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Note note = safeLoadNote("noteId", fromMessage, Permission.RUNNER, authenticationInfo, conn);
    LOGGER.info("Выполнение всех параграфов ноута  noteId: {},  noteUuid: " + note.getUuid(), note.getId());
    List<Paragraph> paragraphsForRun = noteService.getParagraphs(note);

    //filter for run several paragraph
    String json = fromMessage.getOrDefault("paragraphs", null);
    if (!StringUtils.isEmpty(json)) {
      JsonArray paragraphs = new JsonParser().parse(json).getAsJsonArray();
      List<String> paragraphUuids = new ArrayList<>(paragraphs.size());
      for (JsonElement paragraph : paragraphs) {
        String uuid = paragraph.getAsJsonObject().get("id").getAsString();
        paragraphUuids.add(uuid);
      }
      paragraphsForRun = paragraphsForRun.stream()
          .filter(p -> paragraphUuids.contains(p.getUuid()))
          .collect(Collectors.toList());
    }

    noteExecutorService.run(note,
            paragraphsForRun,
            authenticationInfo.getUser(),
            authenticationInfo.getRoles());
  }


  public void runParagraph(final WebSocketSession conn, final SockMessage fromMessage) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, authenticationInfo, conn);
    final Paragraph p = safeLoadParagraph("id", fromMessage, note);

    p.setSelectedText(fromMessage.getOrDefault("selectedText", null));
    LOGGER.info("Выполнение параграфа noteId: {}, noteUuid :" + note.getUuid() + " paragraphId: {} ", note.getId(), p.getId());
    noteExecutorService.run(note,
            Lists.newArrayList(p),
            authenticationInfo.getUser(),
            authenticationInfo.getRoles()
    );
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.PARAGRAPH).put("paragraph", p));
  }

  public void cancelParagraph(final WebSocketSession conn, final SockMessage fromSockMessage) {
    //TODO:KOT duplicate
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Note note = safeLoadNote("noteId", fromSockMessage, Permission.RUNNER, authenticationInfo, conn);
    noteExecutorService.abort(note);
  }
}

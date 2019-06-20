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

import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.handler.AbstractHandler.Permission;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.NoteExecutorService;
import ru.tinkoff.zeppelin.engine.NoteService;

@RestController
@RequestMapping("api/executor/notebook/{noteId}")
public class ExecutorRestApi extends AbstractRestApi {

  private final NoteExecutorService noteExecutorService;

  @Autowired
  protected ExecutorRestApi(
      final NoteService noteService,
      final ConnectionManager connectionManager,
      final NoteExecutorService noteExecutorService) {
    super(noteService, connectionManager);
    this.noteExecutorService = noteExecutorService;
  }

  /**
   * Run paragraph | Endpoint: <b>GET - api/executor/notebook/{noteId}/paragraph/{paragraphId}</b>.
   * @param noteId Notes id
   * @param paragraphId paragraph id
   */
  @GetMapping(value = "/paragraph/{paragraphId}")
  public ResponseEntity runParagraph(
      @PathVariable("noteId") final Long noteId,
      @PathVariable("paragraphId") final Long paragraphId) {
    runParagraphs(noteId, Lists.newArrayList(paragraphId));
    return new JsonResponse(HttpStatus.OK, "Paragraph added to execution queue").build();
  }

  /**
   * Run several paragraphs | Endpoint: <b>POST - api/executor/notebook/{noteId}</b>.
   * Post body: JsonArray of paragraph's ids for execution. Example: <code>[1, 3, 6]</code>
   * @param noteId Notes id
   * @param paragraphIds Paragraph's ids
   */
  @PostMapping
  public ResponseEntity runSeveralParagraphs(
      @PathVariable("noteId") final Long noteId,
      @RequestBody final List<Long> paragraphIds) {
    runParagraphs(noteId, paragraphIds);
    return new JsonResponse(HttpStatus.OK, "Paragraphs added to execution queue").build();
  }

  /**
   * Run all notes paragraphs | Endpoint: <b>GET - api/executor/notebook/{noteId}</b>.
   * @param noteId Notes id
   */
  @GetMapping
  public ResponseEntity runAllNotesParagraphs(@PathVariable("noteId") final String noteId) {
    try {
      final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
      final Note note = secureLoadNote(noteId, Permission.RUNNER);

      noteExecutorService.run(note,
          noteService.getParagraphs(note),
          authenticationInfo.getUser(),
          authenticationInfo.getRoles()
      );
      return new JsonResponse(HttpStatus.OK, "All notes paragraphs added to execution queue")
          .build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
  }

  private void runParagraphs(final Long noteId, final List<Long> paragraphIds) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Note note = secureLoadNote(noteId, Permission.RUNNER);
    final List<Paragraph> paragraphsForRun = noteService.getParagraphs(note).stream()
        .filter(p -> paragraphIds.contains(p.getId()))
        .collect(Collectors.toList());

    noteExecutorService.run(note,
        paragraphsForRun,
        authenticationInfo.getUser(),
        authenticationInfo.getRoles()
    );
  }
}

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

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.CompletionService;
import ru.tinkoff.zeppelin.engine.NoteService;
import ru.tinkoff.zeppelin.engine.forms.FormsProcessor;
import ru.tinkoff.zeppelin.interpreter.InterpreterCompletion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/completion")
public class CompletionRestApi {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompletionRestApi.class);
  private final NoteService noteService;
  private final CompletionService completionService;

  @Autowired
  public CompletionRestApi(final NoteService noteService,
                           final CompletionService completionService) {
    this.noteService = noteService;
    this.completionService = completionService;
  }


  @PostMapping(value = "/{noteId}/{paragraphId}", produces = "application/json")
  public ResponseEntity completion(@PathVariable("noteId") final String noteId,
                                   @PathVariable("paragraphId") final String paragraphId,
                                   @RequestBody final String message) {

    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final Note note = noteService.getNote(noteId);
    final Paragraph p = noteService.getParagraphs(note).stream()
        .filter(e -> e.getUuid().equals(paragraphId))
            .findAny()
            .orElse(null);

    LOGGER.info("Дополнение параграфа paragraphId : {} ноута noteId: {}, noteUuid: {}, message: {} ", p.getId(), note.getId(), note.getUuid(), message);
    final Map<String, String> params = new Gson().fromJson(message,
        new TypeToken<HashMap<String, String>>() {}.getType());

    final String buf = params.get("buf");
    final int cur = (int) Double.parseDouble(params.get("cursor"));
    if (p != null) {
      final FormsProcessor.InjectResponse injectResponse = FormsProcessor.injectFormValues(buf, cur, p.getFormParams());
      final List<InterpreterCompletion> completions = new ArrayList<>();
      completions.addAll(
              completionService.complete(
                      note,
                      p,
                      injectResponse.getPayload(),
                      injectResponse.getCursorPosition(),
                      authenticationInfo.getUser(),
                      authenticationInfo.getRoles())
      );
      return new JsonResponse(HttpStatus.OK, "", completions).build();

    } else {
      //LOGIC ERROR
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Paragraph not found").build();
    }
  }
}

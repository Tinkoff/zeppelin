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

import com.google.gson.JsonObject;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.rest.message.NoteRequest;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.handler.AbstractHandler.Permission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.core.notebook.Scheduler;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.engine.NoteService;
import ru.tinkoff.zeppelin.engine.search.LuceneSearch;
import ru.tinkoff.zeppelin.storage.SchedulerDAO;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/notebook")
public class NotebookRestApi extends AbstractRestApi {

  private static final Logger LOGGER = LoggerFactory.getLogger(NotebookRestApi.class);

  private final LuceneSearch luceneSearch;
  private final SchedulerDAO schedulerDAO;

  @Autowired
  public NotebookRestApi(
      final LuceneSearch luceneSearch,
      final ConnectionManager connectionManager,
      final NoteService noteRepository,
      final SchedulerDAO schedulerDAO) {
    super(noteRepository, connectionManager);
    this.luceneSearch = luceneSearch;
    this.schedulerDAO = schedulerDAO;
  }

  /**
   * List notebooks.
   */
  @PostMapping(produces = "application/json")
  public ResponseEntity getNoteList(@RequestBody final List<String> requestedFields) {
    final List<NoteRequest> response = noteService.getAllNotes().stream()
        .filter(this::userHasReaderPermission)
        .map(NoteRequest::new)
        .collect(Collectors.toList());
    LOGGER.info("Получение списка доступных для чтения ноутов через RestApi(POST)");
    return new JsonResponse(HttpStatus.OK, "List of all available for read notes", response).build();
  }

  /**
   * List notebooks.
   */
  @GetMapping(produces = "application/json")
  public ResponseEntity getNoteList() {
    LOGGER.info("Получение списка доступных для чтения ноутов через RestApi(GET)");
    return getNoteList(null);
  }

  /**
   * Get notebook.
   *
   * @param noteId Id of notebook
   */
  @PostMapping(value = "/{noteId}", produces = "application/json")
  public ResponseEntity getNote(
      @PathVariable("noteId") final long noteId,
      @RequestBody final List<String> requestedFields) {
    NoteRequest noteRequest = new NoteRequest(secureLoadNote(noteId, Permission.READER));
    LOGGER.info("Получение информации о ноуте по ID noteId: {} (POST)", noteId);
    return new JsonResponse(HttpStatus.OK, "Note info", noteRequest).build();
  }

  /**
   * Get notebook.
   *
   * @param noteId Id of notebook
   */
  @GetMapping(value = "/{noteId}", produces = "application/json")
  public ResponseEntity getNote(@PathVariable("noteId") final long noteId) {
    LOGGER.info("Получение информации о ноуте по ID noteId: {} через RestApi(GET) ", noteId);
    return getNote(noteId, null);
  }

  //TODO(SAN) not implemented
  @GetMapping(value = "/{noteId}/export", produces = "application/json")
  public ResponseEntity exportNote(@PathVariable("noteId") final String noteId) {
//    checkIfUserCanRead(noteId, "Insufficient privileges you cannot export this note");
    LOGGER.info("Выгрузка ноута noteId: {} через RestApi", noteId);
    final String exportJson = null;//zeppelinRepository.exportNote(noteId);
    return new JsonResponse(HttpStatus.OK, "", exportJson).build();
  }

  //TODO(SAN) not implemented
  @PostMapping(value = "/import", produces = "application/json")
  public ResponseEntity importNote(final String noteJson) {
    final Note note = null;//zeppelinRepository.importNote(null, noteJson, getServiceContext().getAutheInfo());
    LOGGER.info("Импорт ноута через RestApi");
    return new JsonResponse(HttpStatus.OK, "", note.getId()).build();
  }

  @PostMapping(value = "/create", produces = "application/json")
  public ResponseEntity createNote(@RequestBody final String message) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    LOGGER.info("Создание нового ноута через RestApi");

    try {
      final NoteRequest request = NoteRequest.fromJson(message);
      final Note note = new Note(request.getPath());
      LOGGER.info("noteId: {}, noteUuid: {}", note.getId(), note.getUuid());
      note.getReaders().add(authenticationInfo.getUser());
      note.getRunners().add(authenticationInfo.getUser());
      note.getWriters().add(authenticationInfo.getUser());
      note.getOwners().add(authenticationInfo.getUser());

      note.getReaders().addAll(Configuration.getDefaultReaders());
      note.getRunners().addAll(Configuration.getDefaultRunners());
      note.getWriters().addAll(Configuration.getDefaultWriters());
      note.getOwners().addAll(Configuration.getDefaultOwners());

      noteService.persistNote(note);

      JsonObject response = new JsonObject();
      response.addProperty("note_id", note.getId());
      return new JsonResponse(HttpStatus.OK, "Note created", response).build();
    } catch (final Exception e) {
      throw new IllegalStateException("Failed to create note.", e);
    }
  }

  @GetMapping(value = "/{noteId}/delete", produces = "application/json")
  public ResponseEntity deleteNote(@PathVariable("noteId") final long noteId) {
    final Note note = secureLoadNote(noteId, Permission.OWNER);
    LOGGER.info("Удаление ноута noteId: {}, noteUuid: {} через RestApi", noteId, note.getUuid());
    noteService.deleteNote(note);
    return new JsonResponse(HttpStatus.OK, "Note deleted").build();
  }

  @PostMapping(value = "/{noteId}/clone", produces = "application/json")
  public ResponseEntity cloneNote(
      @PathVariable("noteId") final long noteId,
      @RequestBody final String message) throws IOException, IllegalArgumentException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    Note note = secureLoadNote(noteId, Permission.READER);
    final NoteRequest request = NoteRequest.fromJson(message);

    Note cloneNote = new Note(request.getPath());
    LOGGER.info("Клонирование ноута noteId: {}, noteUuid {},новый ноут noteId: {}, noteUuid: {} через RestApi", note.getId(), note.getUuid(), cloneNote.getId(), cloneNote.getUuid());
    cloneNote.setPath(request.getPath());
    cloneNote.setScheduler(note.getScheduler());
    cloneNote.getReaders().clear();
    cloneNote.getRunners().clear();
    cloneNote.getWriters().clear();
    cloneNote.getOwners().clear();
    cloneNote.getReaders().add(authenticationInfo.getUser());
    cloneNote.getRunners().add(authenticationInfo.getUser());
    cloneNote.getWriters().add(authenticationInfo.getUser());
    cloneNote.getOwners().add(authenticationInfo.getUser());

    cloneNote.getReaders().addAll(Configuration.getDefaultReaders());
    cloneNote.getRunners().addAll(Configuration.getDefaultRunners());
    cloneNote.getWriters().addAll(Configuration.getDefaultWriters());
    cloneNote.getOwners().addAll(Configuration.getDefaultOwners());
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
    JsonObject response = new JsonObject();
    response.addProperty("clone_note_id", cloneNote.getId());
    return new JsonResponse(HttpStatus.OK, "Note cloned", response).build();
  }

  @PostMapping(value = "/{noteId}/update", produces = "application/json")
  public ResponseEntity updateNote(
      @PathVariable("noteId") final long noteId,
      @RequestBody final String message) {
    final NoteRequest request = NoteRequest.fromJson(message);
    final Note note = secureLoadNote(noteId, Permission.OWNER);
    LOGGER.info("Обновление ноута noteId: {}, noteUuid: {} через RestApi", note.getId(), note.getUuid());
    updateIfNotNull(request::getPath, note::setPath);
    updateIfNotNull(request::getOwners, p -> clearAndAdd(p, note.getOwners()));
    updateIfNotNull(request::getWriters, p -> clearAndAdd(p, note.getWriters()));
    updateIfNotNull(request::getRunners, p -> clearAndAdd(p, note.getRunners()));
    updateIfNotNull(request::getReaders, p -> clearAndAdd(p, note.getReaders()));
    noteService.updateNote(note);

    //disable scheduler if note moved in trash
    if (note.getPath().startsWith(Note.TRASH_FOLDER)) {
      Scheduler scheduler = schedulerDAO.getByNote(note.getId());
      if (scheduler != null) {
        scheduler.setEnabled(false);
        schedulerDAO.update(scheduler);
      }
    }

    return new JsonResponse(HttpStatus.OK, "Note updated").build();
  }

  private void clearAndAdd(final Set<String> newPersm, final Set<String> permSet) {
    permSet.clear();
    permSet.addAll(newPersm);
  }

  @GetMapping(value = "/search", produces = "application/json")
  public ResponseEntity search(@RequestParam("q") final String queryTerm) {
    LOGGER.info("Поиск ноутов по запросу {} через RestApi", queryTerm);
    final List<Map<String, String>> result = new ArrayList<>();
    final List<Map<String, String>> notesFound = luceneSearch.query(queryTerm);
    for (int i = 0; i < notesFound.size(); i++) {
      final String[] ids = notesFound.get(i).get("id").split("/", 2);
      final String noteId = ids[0];
      Note note = noteService.getNote(noteId);
      if(userHasReaderPermission(note)) {
        result.add(notesFound.get(i));
      }
    }
    return new JsonResponse(HttpStatus.OK, result).build();
  }
}
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

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.apache.zeppelin.rest.message.NoteRequest;
import org.apache.zeppelin.rest.message.ParagraphRequest;
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

import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/notebook")
public class NotebookRestApi extends AbstractRestApi {

  private static final Logger LOG = LoggerFactory.getLogger(NotebookRestApi.class);

  private final LuceneSearch luceneSearch;
  private final SchedulerDAO schedulerDAO;

  private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

  @Autowired
  public NotebookRestApi(
      final LuceneSearch luceneSearch,
      final ConnectionManager connectionManager,
      final NoteService noteService,
      final SchedulerDAO schedulerDAO) {
    super(noteService, connectionManager);
    this.luceneSearch = luceneSearch;
    this.schedulerDAO = schedulerDAO;
  }

  /**
   * Create new note | Endpoint: <b>POST - /api/notebook</b>
   * @param message Json with parameters for creating note
   *                <table border="1">
   *                <tr><td><b> Name </b></td><td><b> Type </b></td><td><b> Required </b></td><td><b> Description </b></td><tr>
   *                <tr><td>    path    </td>    <td> String   </td>   <td> TRUE  </td>          <td> Notes path </td></tr>
   *                <tr><td>    owners  </td>    <td> [String] </td>   <td> FALSE </td>          <td> Notes owners  roles array </td></tr>
   *                <tr><td>    readers </td>    <td> [String] </td>   <td> FALSE </td>          <td> Notes readers roles array </td></tr>
   *                <tr><td>    runners </td>    <td> [String] </td>   <td> FALSE </td>          <td> Notes runners roles array </td></tr>
   *                <tr><td>    writers </td>    <td> [String] </td>   <td> FALSE </td>          <td> Notes writers roles array </td></tr>
   *                </table>
   * @return New notes id. Example: <code>{"note_id" : 768}</code>
   */
  @PostMapping(produces = "application/json")
  public ResponseEntity createNote(@RequestBody final String message) {
    try {
      final NoteRequest request = NoteRequest.fromJson(message);
      final Note note = request.getAsNote();
      addCurrentUserToOwners(note);
      noteService.persistNote(note);

      final JsonObject response = new JsonObject();
      response.addProperty("note_id", note.getId());
      return new JsonResponse(HttpStatus.OK, "Note created", response).build();
    } catch (final Exception e) {
      throw new IllegalStateException("Failed to create note.", e);
    }
  }

  /**
   * Update note by id | Endpoint: <b>PUT /api/notebook/{noteId}</b>
   * @param noteId Notes id
   * @param message Json with parameters for updating note. Description in {@link #createNote(String)}
   * @return Success message
   */
  @PutMapping(value = "/{noteId}", produces = "application/json")
  public ResponseEntity updateNote(
      @PathVariable("noteId") final long noteId,
      @RequestBody final String message) {
    LOG.info("rename note by JSON {}", message);
    final NoteRequest request = NoteRequest.fromJson(message);
    final Note note = secureLoadNote(noteId, Permission.OWNER);
    updateIfNotNull(request::getPath, note::setPath);
    updateIfNotNull(request::getOwners, p -> clearAndAdd(p, note.getOwners()));
    updateIfNotNull(request::getWriters, p -> clearAndAdd(p, note.getWriters()));
    updateIfNotNull(request::getRunners, p -> clearAndAdd(p, note.getRunners()));
    updateIfNotNull(request::getReaders, p -> clearAndAdd(p, note.getReaders()));
    noteService.updateNote(note);

    //disable scheduler if note moved in trash
    if (note.getPath().startsWith(Note.TRASH_FOLDER)) {
      final Scheduler scheduler = schedulerDAO.getByNote(note.getId());
      if (scheduler != null) {
        scheduler.setEnabled(false);
        schedulerDAO.update(scheduler);
      }
    }

    return new JsonResponse(HttpStatus.OK, "Note updated").build();
  }

  /**
   * Clone note by id | Endpoint: <b>POST - /api/notebook/{noteId}/clone</b>
   * @param noteId Notes id
   * @param message Json with one parameter <code>path</code>. New path for cloned note
   * @return Json with cloned notes id. Example: <code>{"note_id" : 768}</code>
   */
  @SuppressWarnings("Duplicates")
  @PostMapping(value = "/{noteId}/clone", produces = "application/json")
  public ResponseEntity cloneNote(
      @PathVariable("noteId") final long noteId,
      @RequestBody final String message) throws IllegalArgumentException {
    LOG.info("clone note by JSON {}", message);

    final Note note = secureLoadNote(noteId, Permission.READER);
    final NoteRequest request = NoteRequest.fromJson(message);

    Note cloneNote = new Note(request.getPath());
    cloneNote.setPath(request.getPath());
    cloneNote.setScheduler(note.getScheduler());
    cloneNote.getReaders().clear();
    cloneNote.getRunners().clear();
    cloneNote.getWriters().clear();
    cloneNote.getOwners().clear();
    addCurrentUserToOwners(cloneNote);
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
    final JsonObject response = new JsonObject();
    response.addProperty("note_id", cloneNote.getId());
    return new JsonResponse(HttpStatus.OK, "Note cloned", response).build();
  }

  /**
   * Get note info by id | Endpoint: <b>GET - /api/notebook/{noteId}</b>
   * @param noteId Notes id
   * @return All info about note
   */
  @GetMapping(value = "/{noteId:\\d+}", produces = "application/json")
  public ResponseEntity getNote(@PathVariable("noteId") final long noteId) {
    final NoteRequest noteRequest = new NoteRequest(secureLoadNote(noteId, Permission.READER));
    return new JsonResponse(HttpStatus.OK, "Note info", noteRequest).build();
  }

  /**
   * Get note info by uuid | Endpoint: <b>GET - /api/notebook/{noteUUID}</b>
   * @param noteUUID Notes uuid
   * @return All info about note
   */
  @GetMapping(value = "/{noteUUID:\\w+[^0-9]\\w+}", produces = "application/json")
  public ResponseEntity getNoteByUUID(@PathVariable("noteUUID") final String noteUUID) {
    final Note note = noteService.getNote(noteUUID);
    if (!userHasReaderPermission(note)) {
      return new JsonResponse(HttpStatus.UNAUTHORIZED, "You can't see this note").build();
    }
    return new JsonResponse(HttpStatus.OK, "Note info", new NoteRequest(note)).build();
  }

  /**
   * Get all notes | Endpoint: <b>GET - /api/notebook</b>
   * @return List of all the current user’s readable notes
   */
  @GetMapping(produces = "application/json")
  public ResponseEntity getNoteList() {
    final List<JsonObject> response = noteService.getAllNotes().stream()
        .filter(this::userHasReaderPermission)
        .map(n -> {
          JsonObject json = new JsonObject();
          json.addProperty("path", n.getPath());
          json.addProperty("id", n.getId());
          json.addProperty("uuid", n.getUuid());
          return json;
        })
        .collect(Collectors.toList());
    return new JsonResponse(HttpStatus.OK, "List of all available for read notes", response).build();
  }

  /**
   * Export note to json | Endpoint: <b>GET - /api/notebook/{noteId}/export</b>
   * @param noteId Notes id
   * @return Json сontaining a serialized note with paragraphs
   */
  @GetMapping(value = "/{noteId}/export", produces = "application/json")
  public ResponseEntity exportNote(@PathVariable("noteId") final long noteId) {
    final Note note = secureLoadNote(noteId, Permission.READER);
    final NoteRequest noteRequest = new NoteRequest(note);
    final JsonObject json = gson.toJsonTree(noteRequest).getAsJsonObject();

    final List<ParagraphRequest> paragraphs = noteService.getParagraphs(note).stream()
        .map(ParagraphRequest::new)
        .collect(Collectors.toList());

    json.add("paragraphs", gson.toJsonTree(paragraphs));
    return new JsonResponse(HttpStatus.OK, "Note with paragraphs json data", gson.toJson(json)).build();
  }

  /**
   * Import note from json | Endpoint: <b>POST /api/notebook/import</b>
   * @param noteJson Json сontaining a serialized note with paragraphs
   *                 derived from that {@link #exportNote(long)} method
   * @return New notes info
   */
  @PostMapping(value = "/import", produces = "application/json")
  public ResponseEntity importNote(@RequestBody final String noteJson) {
    final NoteRequest request = NoteRequest.fromJson(noteJson);
    Note note = new Note(request.getPath());
    note.setPath(note.getPath() + "_imported");
    note.getOwners().addAll(request.getOwners());
    note.getWriters().addAll(request.getWriters());
    note.getRunners().addAll(request.getRunners());
    note.getReaders().addAll(request.getReaders());
    addCurrentUserToOwners(note);
    note = noteService.persistNote(note);

    final JsonElement paragraphsJson = new JsonParser()
        .parse(noteJson)
        .getAsJsonObject()
        .get("paragraphs");

    final Type type = new TypeToken<List<ParagraphRequest>>() {
    }.getType();
    final List<ParagraphRequest> paragraphs = gson.fromJson(paragraphsJson, type);
    for (final ParagraphRequest paragraphRequest : paragraphs) {
      final Paragraph paragraph = paragraphRequest.getAsParagraph();
      paragraph.setNoteId(note.getId());
      noteService.persistParagraph(note, paragraph);
    }

    return new JsonResponse(HttpStatus.OK, "Imported Note info", new NoteRequest(note)).build();
  }

  private void clearAndAdd(final Set<String> newPersm, final Set<String> permSet) {
    permSet.clear();
    permSet.addAll(newPersm);
  }

  /**
   * Delete note by id | Endpoint: <b>DELETE - /api/notebook/{noteId}</b>
   * @param noteId Notes id
   * @return Success message
   */
  @DeleteMapping(value = "/{noteId}", produces = "application/json")
  public ResponseEntity deleteNote(@PathVariable("noteId") final long noteId) {
    LOG.info("Delete note {} ", noteId);
    final Note note = secureLoadNote(noteId, Permission.OWNER);
    noteService.deleteNote(note);
    return new JsonResponse(HttpStatus.OK, "Note deleted").build();
  }

  //TODO(SAN) Add documentation
  @GetMapping(value = "/search", produces = "application/json")
  public ResponseEntity search(@RequestParam("q") final String queryTerm) {
    LOG.info("Searching notes for: {}", queryTerm);
    final List<Map<String, String>> result = new ArrayList<>();
    final List<Map<String, String>> notesFound = luceneSearch.query(queryTerm);
    for (final Map<String, String> stringStringMap : notesFound) {
      final String[] ids = stringStringMap.get("id").split("/", 2);
      final String noteId = ids[0];
      final Note note = noteService.getNote(noteId);
      if (userHasReaderPermission(note)) {
        result.add(stringStringMap);
      }
    }
    return new JsonResponse(HttpStatus.OK, result).build();
  }

  private void addCurrentUserToOwners(final Note note) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    note.getReaders().add(authenticationInfo.getUser());
    note.getRunners().add(authenticationInfo.getUser());
    note.getWriters().add(authenticationInfo.getUser());
    note.getOwners().add(authenticationInfo.getUser());
  }
}
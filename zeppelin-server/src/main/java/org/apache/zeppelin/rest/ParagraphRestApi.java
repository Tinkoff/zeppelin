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
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.apache.zeppelin.rest.message.ParagraphRequest;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.handler.AbstractHandler.Permission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.NoteService;

@RestController
@RequestMapping("/api/notebook/{noteId}/paragraph")
public class ParagraphRestApi extends AbstractRestApi {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParagraphRestApi.class);

  @Autowired
  protected ParagraphRestApi(
      final NoteService noteService,
      final ConnectionManager connectionManager) {
    super(noteService, connectionManager);
  }

  /**
   * Create new paragraph | Endpoint: <b>POST - /api/notebook/{noteId}/paragraph</b>
   * @param noteId  Notes id
   * @param message Json with parameters for creating note
   *                <table border="1">
   *                <tr><td><b> Name </b></td><td><b> Type </b></td>        <td><b> Required </b></td><td><b> Description </b></td><tr>
   *                <tr>   <td> title      </td> <td> String             </td> <td> FALSE </td>          <td> Title </td>                            </tr>
   *                <tr>   <td> text       </td> <td> String             </td> <td> FALSE </td>          <td> Inner Text </td>                       </tr>
   *                <tr>   <td> shebang    </td> <td> String             </td> <td> TRUE  </td>          <td> interpreter shebang name </td>         </tr>
   *                <tr>   <td> position   </td> <td> Integer            </td> <td> FALSE </td>          <td> Paragraph position index in note </td> </tr>
   *                <tr>   <td> config     </td> <td> Map[String,Object] </td> <td> FALSE </td>          <td> Config map </td>                       </tr>
   *                <tr>   <td> formParams </td> <td> Map[String,Object] </td> <td> FALSE </td>          <td> Description </td>                      </tr>
   *                </table>
   * @return New paragraph's id. Example: <code>{"paragraph_id" : 768}</code>
   */
  @PostMapping(produces = "application/json")
  public ResponseEntity createParagraph(
      @PathVariable("noteId") final long noteId,
      @RequestBody final String message) {
    LOGGER.info("Создание параграфа в ноуте noteId: {} из {} через RestApi", noteId, message);
    final Note note = secureLoadNote(noteId, Permission.OWNER);
    final ParagraphRequest request = ParagraphRequest.fromJson(message);
    final List<Paragraph> paragraphs = noteService.getParagraphs(note);

    // if index == null insert paragraph to end
    if (request.getPosition() == null) {
      request.setPosition(paragraphs.size());
    }

    //check shebang index
    if (request.getPosition() < 0 || request.getPosition() > paragraphs.size()) {
      throw new IndexOutOfBoundsException("Can't insert new paragraph in position " +
          request.getPosition() + ". Paragraph list size is " + paragraphs.size());
    }

    //check shebang
    if (StringUtils.isEmpty(request.getShebang())) {
      throw new InvalidParameterException("Paragraph 'shebang' not defined");
    }

    if (!StringUtils.isEmpty(request.getTitle()) && request.getTitle().length() > 256) {
      throw new InvalidParameterException("Max paragraph title length is 256. Current " +
          request.getTitle().length() +
          ". Title: " + request.getTitle());
    }

    Paragraph paragraph = request.getAsParagraph();
    paragraph.setNoteId(note.getId());
    paragraph = noteService.persistParagraph(note, paragraph);

    for (int i = request.getPosition(); i < paragraphs.size(); i++) {
      final Paragraph p = paragraphs.get(i);
      p.setPosition(i + 1);
      noteService.updateParagraph(note, p);
    }

    final JsonObject response = new JsonObject();
    response.addProperty("paragraph_id", paragraph.getId());
    return new JsonResponse(HttpStatus.OK, "Paragraph created", response).build();
  }

  /**
   * Update paragraph by | Endpoint: <b>PUT - /api/notebook/{noteId}/paragraph/{paragraphId}</b>
   * @param noteId Notes id
   * @param paragraphId Paragraph's id
   * @param message Json with new parameters. By analogy with {@link #createParagraph(long, String)} but every parameters not required.
   * @return Success message
   */
  @PutMapping(value = "/{paragraphId}", produces = "application/json")
  public ResponseEntity updateParagraph(
      @PathVariable("noteId") final long noteId,
      @PathVariable("paragraphId") final long paragraphId,
      @RequestBody final String message) {
    final ParagraphRequest request = ParagraphRequest.fromJson(message);

    final Note note = secureLoadNote(noteId, Permission.WRITER);
    final Paragraph paragraph = getParagraph(note, paragraphId);
    LOGGER.info("Обновление параграфа paragraphId: {}, ноута noteId: {}, noteUuid: {} через RestApi", paragraph.getId(), note.getId(), note.getUuid());

    updateIfNotNull(request::getTitle, paragraph::setTitle);
    updateIfNotNull(request::getText, paragraph::setText);
    updateIfNotNull(request::getConfig, paragraph.getConfig()::putAll);
    updateIfNotNull(request::getShebang, paragraph::setShebang);
    updateIfNotNull(request::getFormParams, paragraph.getFormParams()::putAll);

    // if position in note change
    if (request.getPosition() != null && !request.getPosition().equals(paragraph.getPosition())) {
      moveParagraph(note, paragraphId, request.getPosition());
      paragraph.setPosition(request.getPosition());
    }

    noteService.updateParagraph(note, paragraph);
    return new JsonResponse(HttpStatus.OK, "Paragraph updated").build();
  }

  /** Get paragraph by id | Endpoint: <b>GET - /api/notebook/{noteId}/paragraph/{paragraphId}</b>
   * @param noteId Notes id
   * @param paragraphId Paragraph's id
   * @return Paragraph's info
   */
  @GetMapping(value = "/{paragraphId}", produces = "application/json")
  public ResponseEntity getParagraph(
      @PathVariable("noteId") final long noteId,
      @PathVariable("paragraphId") final long paragraphId) {
    LOGGER.info("Получение параграфа paragraphId: {} ноута noteId: {} через RestApi(GET)", paragraphId, noteId);

    final Note note = secureLoadNote(noteId, Permission.READER);
    final ParagraphRequest response = new ParagraphRequest(getParagraph(note, paragraphId));
    return new JsonResponse(HttpStatus.OK, "Paragraph info", response).build();
  }

  /**
   * Get all paragraph's | Endpoint: <b>GET - /api/notebook/{noteId}/paragraph</b>
   * @param noteId Notes id
   * @return All paragraphs list
   */
  @GetMapping(produces = "application/json")
  public ResponseEntity getAllParagraphs(
      @PathVariable("noteId") final long noteId) {
    LOGGER.info("Получение всех параграфов ноута noteId: {} через RestApi(POST)", noteId);

    final Note note = secureLoadNote(noteId, Permission.READER);
    final List<ParagraphRequest> response = noteService.getParagraphs(note).stream()
        .map(ParagraphRequest::new)
        .collect(Collectors.toList());
    return new JsonResponse(HttpStatus.OK, "All note's paragraphs info", response).build();
  }

  private void moveParagraph(final Note note, final long paragraphId, final int newPosition) {
    final List<Paragraph> paragraphs = noteService.getParagraphs(note);
    LOGGER.info("Изменение номера параграфа paragraphId: {} на {} в ноуте noteId: {}, noteUuid: {} через RestApi",
        paragraphId, newPosition, note.getId(), note.getUuid());

    if (newPosition < 0 || newPosition >= paragraphs.size()) {
      throw new IndexOutOfBoundsException("newPosition " + newPosition + " is out of bounds");
    }

    int oldIndex = -1;
    for (int i = 0; i < paragraphs.size(); i++) {
      if (paragraphs.get(i).getId() == paragraphId) {
        oldIndex = i;
        break;
      }
    }

    final Paragraph paragraph = paragraphs.remove(oldIndex);
    paragraphs.add(newPosition, paragraph);

    for (int i = Math.min(oldIndex, newPosition); i < paragraphs.size(); i++) {
      final Paragraph p = paragraphs.get(i);
      p.setPosition(i);
      noteService.updateParagraph(note, p);
    }
  }

  /**
   * Delete paragraph by id | Endpoint: <b>DELETE - /api/notebook/{noteId}/paragraph/{paragraphId}</b>
   * @param noteId Notes id
   * @param paragraphId Paragraph's id
   * @return Success Message
   */
  @DeleteMapping(value = "/{paragraphId}", produces = "application/json")
  public ResponseEntity deleteParagraph(@PathVariable("noteId") final long noteId,
                                        @PathVariable("paragraphId") final long paragraphId) {
    LOGGER.info("Удаление параграфа paragraphId: {} ноута noteId: {} через RestApi", paragraphId, noteId);
    final Note note = secureLoadNote(noteId, Permission.OWNER);
    final Paragraph paragraph = getParagraph(note, paragraphId);
    noteService.removeParagraph(note, paragraph);
    return new JsonResponse(HttpStatus.OK, "Paragraph deleted").build();
  }

  @PutMapping(value = "/{paragraphId}/set_forms_values", produces = "application/json")
  public ResponseEntity setFormValue(
      @PathVariable("noteId") final long noteId,
      @PathVariable("paragraphId") final long paragraphId,
      @RequestBody final Map<String, Object> formValues) {
    final Note note = secureLoadNote(noteId, Permission.WRITER);
    final Paragraph paragraph = getParagraph(note, paragraphId);
    final Map<String, Object> params = paragraph.getFormParams();
    LOGGER.info("Изменение параметров параграфа paragraphId: {} ноута noteId: {}, noteUuid: {} через RestApi", paragraphId, note.getId(), note.getUuid());

    params.clear();
    params.putAll(formValues);
    noteService.updateParagraph(note, paragraph);
    return new JsonResponse(HttpStatus.OK).build();
  }
}

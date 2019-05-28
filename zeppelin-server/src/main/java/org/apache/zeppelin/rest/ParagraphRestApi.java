package org.apache.zeppelin.rest;

import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
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
import ru.tinkoff.zeppelin.engine.NoteService;

import java.security.InvalidParameterException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

  @PostMapping(value = "/{paragraphId}", produces = "application/json")
  public ResponseEntity getParagraph(
      @PathVariable("noteId") final long noteId,
      @PathVariable("paragraphId") final long paragraphId,
      @RequestBody final List<String> requestedFields) {
    LOGGER.info("Получение параграфа paragraphId: {} ноута noteId: {} через RestApi(POST)", paragraphId, noteId);
    final Note note = secureLoadNote(noteId, Permission.READER);
    final ParagraphRequest response = new ParagraphRequest(getParagraph(note, paragraphId));
    return new JsonResponse(HttpStatus.OK, "Paragraph info", response).build();
  }

  @GetMapping(value = "/{paragraphId}", produces = "application/json")
  public ResponseEntity getParagraph(
      @PathVariable("noteId") final long noteId,
      @PathVariable("paragraphId") final long paragraphId) {
    LOGGER.info("Загрузка параграфа paragraphId: {} ноута noteId: {} через RestApi(GET) ", paragraphId, noteId);
    return getParagraph(noteId, paragraphId, null);
  }

  @PostMapping(value = "/get_all", produces = "application/json")
  public ResponseEntity getAllParagraphs(
      @PathVariable("noteId") final long noteId,
      @RequestBody final List<String> requestedFields) {
    LOGGER.info("Получение всех параграфов ноута noteId: {} через RestApi(POST)", noteId);
    final Note note = secureLoadNote(noteId, Permission.READER);
    List<ParagraphRequest> response = noteService.getParagraphs(note).stream()
        .map(ParagraphRequest::new)
        .collect(Collectors.toList());
    return new JsonResponse(HttpStatus.OK, "All note's paragraphs info", response).build();
  }

  @GetMapping(value = "/get_all", produces = "application/json")
  public ResponseEntity getAllParagraphs(@PathVariable("noteId") final long noteId) {
    LOGGER.info("Получение всех параграфов ноута noteId: {} через RestApi(GET)", noteId);
    return getAllParagraphs(noteId, null);
  }

  @PostMapping(value = "/create", produces = "application/json")
  public ResponseEntity createParagraph(
      @PathVariable("noteId") final long noteId,
      @RequestBody final String message) {
    LOGGER.info("Создание параграфа в ноуте noteId: {} из {} через RestApi", noteId, message);
    final Note note = secureLoadNote(noteId, Permission.OWNER);
    final ParagraphRequest request = ParagraphRequest.fromJson(message);
    final List<Paragraph> paragraphs = noteService.getParagraphs(note);

    // if index == null insert paragraph to end
    final Integer index = request.getPosition() == null ? paragraphs.size() : request.getPosition();

    //check shebang index
    if (index < 0 || index > paragraphs.size()) {
      throw new IndexOutOfBoundsException("Can't insert new paragraph in position " + index +
          ". Paragraph list size is " + paragraphs.size());
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

    Paragraph paragraph = new Paragraph();
    paragraph.setNoteId(note.getId());
    paragraph.setTitle(request.getTitle());
    paragraph.setText(request.getText());
    paragraph.setShebang(request.getShebang());
    paragraph.setCreated(LocalDateTime.now());
    paragraph.setUpdated(LocalDateTime.now());
    paragraph.setPosition(index);
    updateIfNotNull(request::getConfig, paragraph.getConfig()::putAll);
    paragraph = noteService.persistParagraph(note, paragraph);

    for (int i = index; i < paragraphs.size(); i++) {
      final Paragraph p = paragraphs.get(i);
      p.setPosition(i + 1);
      noteService.updateParagraph(note, p);
    }

    JsonObject response = new JsonObject();
    response.addProperty("paragraph_id", paragraph.getId());
    return new JsonResponse(HttpStatus.OK, "Paragraph created", response).build();
  }

  @PostMapping(value = "/{paragraphId}/update", produces = "application/json")
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

  private void moveParagraph(final Note note, final long paragraphId, final int newPosition) {
    List<Paragraph> paragraphs = noteService.getParagraphs(note);
    LOGGER.info("Изменение номера параграфа paragraphId: {} на {} в ноуте noteId: {}, noteUuid: {} через RestApi", paragraphId, newPosition, note.getId(), note.getUuid());
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

    Paragraph paragraph = paragraphs.remove(oldIndex);
    paragraphs.add(newPosition, paragraph);

    for (int i = Math.min(oldIndex, newPosition); i < paragraphs.size(); i++) {
      final Paragraph p = paragraphs.get(i);
      p.setPosition(i);
      noteService.updateParagraph(note, p);
    }
  }

  @GetMapping(value = "/{paragraphId}/delete", produces = "application/json")
  public ResponseEntity deleteParagraph(@PathVariable("noteId") final long noteId,
      @PathVariable("paragraphId") final long paragraphId) {
    LOGGER.info("Удаление параграфа paragraphId: {} ноута noteId: {} через RestApi", paragraphId, noteId);
    final Note note = secureLoadNote(noteId, Permission.OWNER);
    Paragraph paragraph = getParagraph(note, paragraphId);
    noteService.removeParagraph(note, paragraph);
    return new JsonResponse(HttpStatus.OK, "Paragraph deleted").build();
  }

  @PostMapping(value = "/{paragraphId}/set_forms_values", produces = "application/json")
  public ResponseEntity setFormValue(
      @PathVariable("noteId") final long noteId,
      @PathVariable("paragraphId") final long paragraphId,
      @RequestBody final Map<String, Object> formValues
  ) {
    Note note = secureLoadNote(noteId, Permission.WRITER);
    Paragraph paragraph = getParagraph(note, paragraphId);
    LOGGER.info("Изменение параметров параграфа paragraphId: {} ноута noteId: {}, noteUuid: {} через RestApi", paragraphId, note.getId(), note.getUuid());
    Map<String, Object> params = paragraph.getFormParams();
    params.clear();
    params.putAll(formValues);
    noteService.updateParagraph(note, paragraph);
    return new JsonResponse(HttpStatus.OK).build();
  }
}

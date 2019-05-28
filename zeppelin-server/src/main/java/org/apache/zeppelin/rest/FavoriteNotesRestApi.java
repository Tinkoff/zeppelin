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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.engine.NoteService;

@RestController
@RequestMapping("/api/favorite_notes")
public class FavoriteNotesRestApi {
  private static final Logger LOGGER = LoggerFactory.getLogger(FavoriteNotesRestApi.class);

  private static final Integer RECENT_NOTEBOOK_COUNT = 7;
  private static final Integer SAVE_ON_DISK_INTERVAL = 5; // minutes

  private final ScheduledExecutorService saveExecServ;
  private final NoteService noteRepo;

  private File dataFile;
  private Map<String, Map<String, Set<String>>> usersNotes;

  @Autowired
  public FavoriteNotesRestApi(final NoteService noteRepo) {
    this.noteRepo = noteRepo;
    this.saveExecServ = Executors.newSingleThreadScheduledExecutor();
  }

  @PostConstruct
  public void init() throws IOException {
    dataFile = new File("favorite-notes/meta.json");
    dataFile.getParentFile().mkdirs();
    dataFile.createNewFile();
    usersNotes = loadNotesList();
    if (usersNotes == null) {
      usersNotes = new ConcurrentHashMap<>();
    }
    clearIncorrectNotesIds();

    Runtime.getRuntime().addShutdownHook(new Thread(this::saveNotesList));
    saveExecServ.scheduleWithFixedDelay(this::saveNotesList, SAVE_ON_DISK_INTERVAL, SAVE_ON_DISK_INTERVAL, TimeUnit.MINUTES);
  }

  private synchronized Map<String, Map<String, Set<String>>> loadNotesList() {
    final Type type = new TypeToken<ConcurrentHashMap<String, HashMap<String, LinkedHashSet<String>>>>() {
    }.getType();
    try (final FileReader fileReader = new FileReader(dataFile)) {
      final Gson gson = new Gson();
      final JsonReader reader = new JsonReader(fileReader);
      return gson.fromJson(reader, type);
    } catch (final IOException e) {
      LOGGER.info("Json file with favorites notes not found!");
      return new ConcurrentHashMap<>();
    }
  }

  private synchronized void saveNotesList() {
    clearIncorrectNotesIds();
    try (final FileWriter fileWriter = new FileWriter(dataFile)) {
      final Gson gson = new GsonBuilder().setPrettyPrinting().create();
      gson.toJson(usersNotes, fileWriter);
    } catch (final IOException e) {
      LOGGER.error("Error when try save 'favorites notes list file'", e);
    }
  }

  private boolean noteIdIsCorrect(final String noteId) {
    final Note note = noteRepo.getNote(noteId);
    if (note == null) {
      return false;
    }
    return !note.isTrashed();
  }

  private synchronized void clearIncorrectNotesIds() {
    for (final Map<String, Set<String>> userList : usersNotes.values()) {
      for (final Set<String> noteType : userList.values()) {
        noteType.removeIf(id -> !noteIdIsCorrect(id));
      }
    }
  }

  @GetMapping(value = "/get_notes_ids", produces = "application/json")
  public ResponseEntity getNotesIds(@RequestParam("username") final String username) {
    LOGGER.info("Получение Uuid-идентификаторов избранных и недавних ноутов");
    try {
      final HashMap<String, Set> idsMap = new HashMap<>(2);
      Set<String> favoriteSet = Collections.emptySet();
      Set<String> recentSet = Collections.emptySet();
      try {
        favoriteSet = Objects.requireNonNull(usersNotes.get(username).get("favorite"));
      } catch (final Exception ignore) {
      }
      try {
        recentSet = Objects.requireNonNull(usersNotes.get(username).get("recent"));
      } catch (final Exception ignore) {
      }
      idsMap.put("favorite", favoriteSet);
      idsMap.put("recent", recentSet);
      return new JsonResponse(HttpStatus.OK, "", idsMap).build();
    } catch (final NullPointerException ignore) {
      return new JsonResponse(HttpStatus.OK, "", Collections.emptyList()).build();
    }
  }

  @GetMapping(value = "/set_note_status", produces = "application/json")
  public ResponseEntity setNoteStatus(@RequestParam("username") final String username,
                                      @RequestParam("note_id") final String noteId,
                                      @RequestParam("note_type") final String noteType,
                                      @RequestParam("note_action") final String noteAction) {
    LOGGER.info("Добавление/удаление ноута noteId: {} из избранных/недавних {}", noteId, noteType);
    //TODO(SAN) add " || !SecurityUtils.getSubject().getPrincipal().equals(username)"
    if (username == null || username.isEmpty()) {
      return new JsonResponse(HttpStatus.BAD_REQUEST, "username no correct").build();
    }
    if (noteId == null || noteId.isEmpty()) {
      return new JsonResponse(HttpStatus.BAD_REQUEST, "note_id no correct").build();
    }
    if (noteType == null || noteType.isEmpty() || !(noteType.equals("favorite") || noteType.equals("recent"))) {
      return new JsonResponse(HttpStatus.BAD_REQUEST, "note_type no correct").build();
    }
    if (noteAction == null || noteAction.isEmpty() || !(noteAction.equals("add") || noteAction.equals("remove"))) {
      return new JsonResponse(HttpStatus.BAD_REQUEST, "note_action no correct").build();
    }

    final Map<String, Set<String>> notesMap = usersNotes.computeIfAbsent(username, k -> new HashMap<>());
    final Set<String> idsSet = notesMap.computeIfAbsent(noteType, k -> new LinkedHashSet<>());
    if (noteAction.equals("add") && noteIdIsCorrect(noteId)) {
      idsSet.remove(noteId);
      idsSet.add(noteId);
      while (noteType.equals("recent") && idsSet.size() > RECENT_NOTEBOOK_COUNT) {
        idsSet.remove(idsSet.iterator().next());
      }
    }
    if (noteAction.equals("remove")) {
      idsSet.remove(noteId);

      new Thread(() -> {
        try {
          Thread.sleep(1000);
          if (!noteIdIsCorrect(noteId)) {
            clearIncorrectNotesIds();
          }
        } catch (InterruptedException ignore) {
        }
      }).start();
    }

    return new JsonResponse(HttpStatus.OK, "is_" + noteType).build();
  }
}
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.storage.FavoriteNotesDAO;
import ru.tinkoff.zeppelin.storage.RecentNotesDAO;

@RestController
@RequestMapping("/api/marked_notes")
public class MarkedNotesRestApi {
  private static final Logger LOGGER = LoggerFactory.getLogger(MarkedNotesRestApi.class);
  private static final Integer RECENT_NOTES_CLEANUP_INTERVAL = 10; // minutes

  private final FavoriteNotesDAO favoriteNotesDAO;
  private final RecentNotesDAO recentNotesDAO;
  private final ScheduledExecutorService saveExecServ;

  @Autowired
  public MarkedNotesRestApi(final FavoriteNotesDAO favoriteNotesDAO,
                            final RecentNotesDAO recentNotesDAO) {
    this.favoriteNotesDAO = favoriteNotesDAO;
    this.recentNotesDAO = recentNotesDAO;
    this.saveExecServ = Executors.newSingleThreadScheduledExecutor();
  }

  @PostConstruct
  public void init() {
    saveExecServ.scheduleWithFixedDelay(this::recentNotesCleanup,
        RECENT_NOTES_CLEANUP_INTERVAL, RECENT_NOTES_CLEANUP_INTERVAL, TimeUnit.MINUTES);
  }

  private synchronized void recentNotesCleanup() {
    try {
      recentNotesDAO.cleanup();
    } catch (final Exception e) {
      // skip
    }
  }

  @GetMapping(value = "/get_notes_ids", produces = "application/json")
  public ResponseEntity getNotesIds(@RequestParam("username") final String username) {
    try {
      LOGGER.info("Получение Uuid-идентификаторов избранных и недавних ноутов");

      final HashMap<String, Set> idsMap = new HashMap<>(2);
      idsMap.put("favorite", new HashSet<>(favoriteNotesDAO.getAll(username)));
      idsMap.put("recent", new LinkedHashSet<>(recentNotesDAO.getAll(username)));

      return new JsonResponse(HttpStatus.OK, "", idsMap).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
  }

  @GetMapping(value = "/set_note_status", produces = "application/json")
  public ResponseEntity setNoteStatus(@RequestParam("username") final String username,
                                      @RequestParam("note_id") final String noteId,
                                      @RequestParam("note_type") final String noteType,
                                      @RequestParam("note_action") final String noteAction) {
    LOGGER.info("Добавление/удаление ноута noteId: {} из избранных/недавних {}", noteId, noteType);

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

    if (noteAction.equals("add")) {
      if (noteType.equals("favorite")) {
        favoriteNotesDAO.persist(noteId, username);
      } else {
        recentNotesDAO.persist(noteId, username);
      }
    } else if (noteType.equals("favorite")) {
      favoriteNotesDAO.remove(noteId, username);
    }

    return new JsonResponse(HttpStatus.OK, "is_" + noteType).build();
  }
}
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

import com.google.common.collect.Sets;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.rest.exception.NoteNotFoundException;
import ru.tinkoff.zeppelin.storage.SchedulerDAO;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Scheduler;
import ru.tinkoff.zeppelin.engine.NoteService;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

@RestController
@RequestMapping("api")
public class CronRestApi {

  private static final Logger LOGGER = LoggerFactory.getLogger(CronRestApi.class);

  private final NoteService noteRepository;
  private final SchedulerDAO schedulerDAO;
  private final ConnectionManager connectionManager;

  public CronRestApi(
      final NoteService noteRepository,
      final SchedulerDAO schedulerDAO,
      ConnectionManager connectionManager) {
    this.noteRepository = noteRepository;
    this.schedulerDAO = schedulerDAO;
    this.connectionManager = connectionManager;
  }

  /**
   * Register cron job REST API.
   *
   * @return JSON with status.OK
   */
  @PostMapping(value = "/notebook/{noteId}/cron", produces = "application/json")
  public ResponseEntity registerCronJob(
      @PathVariable("noteId") final String noteIdParam,
      @RequestBody Map<String, String> params
  ) throws IllegalArgumentException {

    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    String expression = params.get("expression");
    String isEnableParam = params.get("enable");
    long noteId = Long.parseLong(noteIdParam);
    boolean isEnable = isEnableParam == null ? true : Boolean.valueOf(isEnableParam);

    final Note note = noteRepository.getNote(noteId);
    LOGGER.info("Регистрация задания планировщика для ноута noteId: {}, noteUuid: {}  с расписанием {}, флаг включения = {}", note.getId(), note.getUuid(), expression, isEnableParam);
    checkIfNoteIsNotNull(note);
    checkIfUserCanRun(noteId, "Insufficient privileges you cannot set a cron job for this note");

    final CronExpression cronExpression;
    try {
      cronExpression = new CronExpression(expression);
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.BAD_REQUEST, "wrong cron expressions.").build();
    }

    Scheduler scheduler = schedulerDAO.getByNote(note.getId());

    if (scheduler != null) {

      scheduler.setExpression(expression);
      scheduler.setEnabled(isEnable);
      if(isEnable) {
        final Date nextExecutionDate = cronExpression.getNextValidTimeAfter(new Date());
        final LocalDateTime nextExecution = LocalDateTime
                .ofInstant(nextExecutionDate.toInstant(), ZoneId.systemDefault());
        scheduler.setNextExecution(nextExecution);
      }
      scheduler = schedulerDAO.update(scheduler);
    } else {
      final Date nextExecutionDate = cronExpression.getNextValidTimeAfter(new Date());
      final LocalDateTime nextExecution = LocalDateTime
          .ofInstant(nextExecutionDate.toInstant(), ZoneId.systemDefault());
      scheduler = new Scheduler(
          null,
          note.getId(),
          isEnable,
          expression,
          authenticationInfo.getUser(),
          new HashSet<>(authenticationInfo.getRoles()),
          nextExecution,
          nextExecution
      );
      scheduler = schedulerDAO.persist(scheduler);
    }

    HashMap<String, Object> responce = new HashMap<>(2);
    responce.put("newCronExpression", scheduler.getExpression());
    responce.put("enable", scheduler.isEnabled());
    SockMessage message = new SockMessage(Operation.NOTE_UPDATED);
    message.put("path", note.getPath());
    message.put("config", note.getFormParams());
    message.put("info", null);
    connectionManager.broadcast(note.getId(), message);
    return new JsonResponse(HttpStatus.OK, responce).build();
  }

  /**
   * Check valid cron expression REST API.
   *
   * @return JSON with status.OK
   */
  @GetMapping(value = "/cron/check_valid", produces = "application/json")
  public ResponseEntity checkCronExpression(@RequestParam("cronExpression") final String expression)
      throws IllegalArgumentException {
    LOGGER.info("Проверка правильности выражения, с помощью которого задается расписание планировщика {}",  expression);
    if (!CronExpression.isValidExpression(expression)) {
      return new JsonResponse(HttpStatus.OK, "invalid").build();
    }
    return new JsonResponse(HttpStatus.OK, "valid").build();
  }

  /**
   * Remove cron job REST API.
   *
   * @param noteIdParam ID of Note
   * @return JSON with status.OK
   */
  @DeleteMapping(value = "/notebook/{noteId}/cron", produces = "application/json")
  public ResponseEntity removeCronJob(@PathVariable("noteId") final String noteIdParam)
      throws IOException, IllegalArgumentException {

    long noteId = Long.parseLong(noteIdParam);

    final Note note = noteRepository.getNote(noteId);
    LOGGER.info("Удаление задания планировщика для ноута noteId: {}, noteUuid: {}", note.getId(), note.getUuid());
    checkIfNoteIsNotNull(note);
    checkIfUserIsOwner(noteId,
        "Insufficient privileges you cannot remove this cron job from this note");

    note.getScheduler().setEnabled(false);
    //zeppelinRepository.refreshCron(note.getNoteId());
    return new JsonResponse(HttpStatus.OK).build();
  }

  /**
   * Get cron job REST API.
   *
   * @param noteIdParam ID of Note
   * @return JSON with status.OK
   */
  @GetMapping(value = "/notebook/{noteId}/cron", produces = "application/json")
  public ResponseEntity getCronJob(@PathVariable("noteId") final String noteIdParam)
      throws IllegalArgumentException {

    long noteId = Long.parseLong(noteIdParam);
    final Note note = noteRepository.getNote(noteId);
    LOGGER.info("Получение данных о планировщике для ноута noteId: {}, noteUuid: {} ", note.getId(), note.getUuid());
    checkIfNoteIsNotNull(note);
    checkIfUserCanRead(noteId, "Insufficient privileges you cannot get cron information");
    Scheduler scheduler = schedulerDAO.getByNote(note.getId());
    final Map<String, Object> response = new HashMap<>();
    response.put("cron", scheduler == null ? null : scheduler.getExpression());
    response.put("enable", scheduler == null ? false : scheduler.isEnabled());

    return new JsonResponse(HttpStatus.OK, response).build();
  }

  /**
   * Check if the current user own the given note.
   */
  private void checkIfUserIsOwner(final Long noteId, final String errorMsg) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Set<String> userAndRoles = Sets.newHashSet();
    userAndRoles.add(authenticationInfo.getUser());
    userAndRoles.addAll(authenticationInfo.getRoles());
    //if (!notePermissionsService.isOwner(userAndRoles, noteId)) {
    //  throw new ForbiddenException(errorMsg);
    //}
  }

  /**
   * Check if the current user can access (at least he have to be reader) the given note.
   */
  private void checkIfUserCanRead(final Long noteId, final String errorMsg) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Set<String> userAndRoles = Sets.newHashSet();
    userAndRoles.add(authenticationInfo.getUser());
    userAndRoles.addAll(authenticationInfo.getRoles());
    //if (!notePermissionsService.hasReadAuthorization(userAndRoles, noteId)) {
    //  throw new ForbiddenException(errorMsg);
    //}
  }

  /**
   * Check if the current user can run the given note.
   */
  private void checkIfUserCanRun(final Long noteId, final String errorMsg) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Set<String> userAndRoles = Sets.newHashSet();
    userAndRoles.add(authenticationInfo.getUser());
    userAndRoles.addAll(authenticationInfo.getRoles());
    //if (!notePermissionsService.hasRunAuthorization(userAndRoles, noteId)) {
    // throw new ForbiddenException(errorMsg);
    //}
  }

  private void checkIfNoteIsNotNull(final Note note) {
    if (note == null) {
      throw new NoteNotFoundException("note not found");
    }
  }
}

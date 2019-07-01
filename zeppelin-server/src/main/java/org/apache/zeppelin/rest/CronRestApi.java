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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.apache.zeppelin.websocket.handler.AbstractHandler.Permission;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Scheduler;
import ru.tinkoff.zeppelin.engine.NoteEventService;
import ru.tinkoff.zeppelin.engine.NoteService;
import ru.tinkoff.zeppelin.storage.SchedulerDAO;

@RestController
@RequestMapping("api")
public class CronRestApi extends AbstractRestApi {

  private static final Logger LOGGER = LoggerFactory.getLogger(CronRestApi.class);

  private final SchedulerDAO schedulerDAO;
  private final ConnectionManager connectionManager;
  private final NoteEventService noteEventService;

  public CronRestApi(
      final NoteService noteService,
      final SchedulerDAO schedulerDAO,
      final ConnectionManager connectionManager,
      final NoteEventService noteEventService) {
    super(noteService, connectionManager);
    this.schedulerDAO = schedulerDAO;
    this.connectionManager = connectionManager;
    this.noteEventService = noteEventService;
  }

  /**
   * Register cron job | Endpoint: <b>POST - /api/notebook/{noteId}/cron</b>.
   * Json object:
   * <table border="1">
   *   <tr><td><b> Name </b></td><td><b> Type </b></td><td><b> Required </b></td><td><b> Description </b></td><tr>
   *   <tr><td>    expression </td> <td> String    </td>  <td> FALSE  </td>         <td> Cron expression text </td></tr>
   *   <tr><td>    enable     </td> <td> Boolean   </td>  <td> FALSE  </td>         <td> Cron enable status (Default: true) </td></tr>
   * </table>
   * @return JSON with status.OK
   */
  @PutMapping(value = "/notebook/{noteId}/cron", produces = "application/json")
  public ResponseEntity registerCronJob(
      @PathVariable("noteId") final String noteIdParam,
      @RequestBody final Map<String, String> params) throws IllegalArgumentException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    final String expression = params.get("expression");
    final Boolean isEnable = params.get("enable") != null ? Boolean.parseBoolean(params.get("enable")) : null;
    final boolean doUpdateUser = params.get("doUpdateUser") != null && Boolean.parseBoolean(params.get("doUpdateUser"));

    final long noteId = Long.parseLong(noteIdParam);
    final Note note = secureLoadNote(noteId, Permission.OWNER);
    Scheduler scheduler = schedulerDAO.getByNote(note.getId());

    LOGGER.info("Регистрация задания планировщика для ноута noteId: {}, noteUuid: {}  с расписанием {}, флаг включения = {}", note.getId(), note.getUuid(), expression, isEnable);
    if (scheduler == null && expression == null) {
      return new JsonResponse(HttpStatus.BAD_REQUEST,"No expression found").build();
    }

    boolean isNewCronScheduler = false;
    if (scheduler == null) {
      isNewCronScheduler = true;
      scheduler = new Scheduler(note.getId());
      scheduler.setEnabled(true);
    }

    // get CronExpression
    final CronExpression cronExpression;
    try {
      cronExpression = new CronExpression(expression == null ? scheduler.getExpression() : expression);
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.BAD_REQUEST, "wrong cron expressions").build();
    }

    final Scheduler oldScheduler = isNewCronScheduler ? null : scheduler.getScheduler();

    updateIfNotNull(() -> expression, scheduler::setExpression);
    updateIfNotNull(() -> isEnable, scheduler::setEnabled);

    // update user and roles
    if (isNewCronScheduler || doUpdateUser) {
      scheduler.setUser(authenticationInfo.getUser());
      scheduler.setRoles(authenticationInfo.getRoles());
    }

    // update execution date
    final Date nextExecutionDate = cronExpression.getNextValidTimeAfter(new Date());
    final LocalDateTime nextExecution = LocalDateTime
        .ofInstant(nextExecutionDate.toInstant(), ZoneId.systemDefault());
    scheduler.setNextExecution(nextExecution);
    if (scheduler.getLastExecution() == null) {
      scheduler.setLastExecution(scheduler.getNextExecution());
    }


    scheduler = isNewCronScheduler ? schedulerDAO.persist(scheduler) : schedulerDAO.update(scheduler);
    noteEventService.noteScheduleChange(note, oldScheduler);

    // broadcast event
    final SockMessage message = new SockMessage(Operation.NOTE_UPDATED);
    message.put("path", note.getPath());
    message.put("config", note.getFormParams());
    message.put("info", null);
    connectionManager.broadcast(note.getId(), message);

    // send response
    final HashMap<String, Object> response = new HashMap<>(2);
    response.put("newCronExpression", scheduler.getExpression());
    response.put("enable", scheduler.isEnabled());
    return new JsonResponse(HttpStatus.OK, response).build();
  }

  @PutMapping(value = "/notebook/{noteId}/cron/user", produces = "application/json")
  public ResponseEntity updateCronUser(
      @PathVariable("noteId") final String noteIdParam,
      @RequestBody final Map<String, Object> params) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();

    if ("anonymous".equals(authenticationInfo.getUser())) {
      new JsonResponse(HttpStatus.UNAUTHORIZED, "Only anonymous support this method").build();
    }

    final String user = (String) params.get("user");
    //noinspection unchecked,MismatchedQueryAndUpdateOfCollection
    final Set<String> roles =  params.containsKey("roles") ? new HashSet<>((ArrayList<String>) params.get("roles")) : Collections.emptySet();

    if (StringUtils.isEmpty(user)) {
      new JsonResponse(HttpStatus.BAD_REQUEST, "'user' parameter is empty").build();
    }

    final long noteId = Long.parseLong(noteIdParam);
    final Scheduler scheduler = schedulerDAO.getByNote(noteId);

    scheduler.setUser(user);
    scheduler.setRoles(roles);
    schedulerDAO.update(scheduler);

    return new JsonResponse(HttpStatus.OK, "cron user changed").build();
  }

  /**
   * Check valid cron expression | Endpoint: <b>POST - /api/cron/check_valid</b>
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
   * Get cron job REST API | Endpoint: <b>POST - /api/notebook/{noteId}/cron</b>
   *
   * @param noteIdParam ID of Note
   * @return JSON with status.OK
   */
  @GetMapping(value = "/notebook/{noteId}/cron", produces = "application/json")
  public ResponseEntity getCronJob(@PathVariable("noteId") final String noteIdParam)
      throws IllegalArgumentException {
    final long noteId = Long.parseLong(noteIdParam);
    final Note note = secureLoadNote(noteId, Permission.READER);
    LOGGER.info("Получение данных о планировщике для ноута noteId: {}, noteUuid: {} ", note.getId(), note.getUuid());
    final Scheduler scheduler = schedulerDAO.getByNote(note.getId());
    final Map<String, Object> response = new HashMap<>();
    response.put("enable", scheduler != null && scheduler.isEnabled());
    response.put("cron", scheduler == null ? null : scheduler.getExpression());
    response.put("user", scheduler == null ? null : scheduler.getUser());
    response.put("roles", scheduler == null ? Collections.emptySet() : scheduler.getRoles());

    return new JsonResponse(HttpStatus.OK, response).build();
  }

  /**
   * Remove cron job REST API | Endpoint: <b>DELETE - /api/notebook/{noteId}/cron</b>
   *
   * @param noteIdParam ID of Note
   * @return JSON with status.OK
   */
  @DeleteMapping(value = "/notebook/{noteId}/cron", produces = "application/json")
  public ResponseEntity removeCronJob(@PathVariable("noteId") final String noteIdParam) {
    final long noteId = Long.parseLong(noteIdParam);
    final Note note = secureLoadNote(noteId, Permission.OWNER);
    LOGGER.info("Получение данных о планировщике для ноута noteId: {}, noteUuid: {} ", note.getId(), note.getUuid());
    final Scheduler scheduler = schedulerDAO.getByNote(note.getId());
    schedulerDAO.remove(scheduler);
    return new JsonResponse(HttpStatus.OK).build();
  }
}

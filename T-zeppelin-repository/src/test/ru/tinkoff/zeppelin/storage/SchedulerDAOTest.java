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
package ru.tinkoff.zeppelin.storage;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Scheduler;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.*;

public class SchedulerDAOTest extends AbstractTest {

  private static SchedulerDAO schedulerDAO;

  @BeforeClass
  public static void init() {
    schedulerDAO = new SchedulerDAO(jdbcTemplate);
  }

  @Before
  public void forEach() {
    createNote("temp_1", Notes.UUID_1);
    createNote("temp_2", Notes.UUID_2);
    createNote("temp_3", Notes.UUID_3);
  }

  @After
  public void afterEach() {
    deleteNote(Notes.UUID_1);
    deleteNote(Notes.UUID_2);
    deleteNote(Notes.UUID_3);
  }

  @Test
  public void persist() {
    final Note note = getNote(Notes.UUID_1);
    final Scheduler scheduler = new Scheduler(
        null,
        note.getId(),
        true,
        "1 * * * * ?",
        Person.USERNAME_3,
        new HashSet<>(Arrays.asList(Person.USERNAME_1, Person.USERNAME_2)),
        LocalDateTime.now().minusDays(1),
        LocalDateTime.now().plusDays(1)
    );
    schedulerDAO.persist(scheduler);
    assertNotNull(scheduler.getId());
  }

  @Test
  public void update() {
    final Note note = getNote(Notes.UUID_1);
    final Scheduler scheduler = new Scheduler(
        null,
        note.getId(),
        true,
        "1 * * * * ?",
        Person.USERNAME_3,
        new HashSet<>(Arrays.asList(Person.USERNAME_1, Person.USERNAME_2)),
        LocalDateTime.now().minusDays(1),
        LocalDateTime.now().plusDays(1)
    );
    schedulerDAO.persist(scheduler);

    scheduler.setExpression("2 * * * * ?");
    schedulerDAO.update(scheduler);

    final Scheduler dbScheduler = schedulerDAO.get(scheduler.getId());
    assertEquals(scheduler, dbScheduler);
  }

  @Test
  public void get() {
    final Note note = getNote(Notes.UUID_1);
    final Scheduler scheduler = new Scheduler(
        null,
        note.getId(),
        true,
        "1 * * * * ?",
        Person.USERNAME_3,
        new HashSet<>(Arrays.asList(Person.USERNAME_1, Person.USERNAME_2)),
        LocalDateTime.now().minusDays(1),
        LocalDateTime.now().plusDays(1)
    );
    schedulerDAO.persist(scheduler);

    final Scheduler dbScheduler = schedulerDAO.get(scheduler.getId());
    assertEquals(scheduler, dbScheduler);
  }

  @Test
  public void getByNote() {
    final Note note = getNote(Notes.UUID_1);
    final Scheduler scheduler = new Scheduler(
        null,
        note.getId(),
        true,
        "1 * * * * ?",
        Person.USERNAME_3,
        new HashSet<>(Arrays.asList(Person.USERNAME_1, Person.USERNAME_2)),
        LocalDateTime.now().minusDays(1),
        LocalDateTime.now().plusDays(1)
    );
    schedulerDAO.persist(scheduler);

    final Scheduler dbScheduler = schedulerDAO.getByNote(note.getId());
    assertEquals(scheduler, dbScheduler);
  }

  @Test
  public void getReadyToExecute() {
    final Note noteNow = getNote(Notes.UUID_1);
    final Scheduler schedulerNow = new Scheduler(
        null,
        noteNow.getId(),
        true,
        "1 * * * * ?",
        Person.USERNAME_3,
        new HashSet<>(Arrays.asList(Person.USERNAME_1, Person.USERNAME_2)),
        LocalDateTime.now().minusDays(1),
        LocalDateTime.now()
    );
    schedulerDAO.persist(schedulerNow);

    final Note noteTomorrow = getNote(Notes.UUID_2);
    final Scheduler scheduleTomorrow = new Scheduler(
        null,
        noteTomorrow.getId(),
        true,
        "1 * * * * ?",
        Person.USERNAME_3,
        new HashSet<>(Arrays.asList(Person.USERNAME_1, Person.USERNAME_2)),
        LocalDateTime.now().minusDays(1),
        LocalDateTime.now().plusDays(1)
    );
    schedulerDAO.persist(scheduleTomorrow);

    final Note noteDisable = getNote(Notes.UUID_3);
    final Scheduler scheduleDisable = new Scheduler(
        null,
        noteDisable.getId(),
        true,
        "1 * * * * ?",
        Person.USERNAME_3,
        new HashSet<>(Arrays.asList(Person.USERNAME_1, Person.USERNAME_2)),
        LocalDateTime.now().minusDays(1),
        LocalDateTime.now().plusDays(1)
    );
    scheduleDisable.setEnabled(false);
    schedulerDAO.persist(scheduleDisable);

    final List<Scheduler> readyToExecute = schedulerDAO.getReadyToExecute(LocalDateTime.now());
    assertEquals(1, readyToExecute.size());
    assertTrue(readyToExecute.contains(schedulerNow));
  }
}
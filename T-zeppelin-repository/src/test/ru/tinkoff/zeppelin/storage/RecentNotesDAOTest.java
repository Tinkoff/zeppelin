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

import org.junit.*;

import java.util.List;

import static org.junit.Assert.*;

public class RecentNotesDAOTest extends AbstractTest {

  private static RecentNotesDAO recentNotesDAO;

  @BeforeClass
  public static void init() {
    recentNotesDAO = new RecentNotesDAO(jdbcTemplate);
  }

  @Before
  public void forEach() {
    createNote("temp_1", Notes.UUID_1);
  }

  @After
  public void afterEach() {
    deleteNote(Notes.UUID_1);
    deleteNote(Notes.UUID_2);
    deleteNote(Notes.UUID_3);
    deleteNote(Notes.UUID_4);
    deleteNote(Notes.UUID_5);
    deleteNote(Notes.UUID_6);
  }

  @Test
  public void persist() {
    recentNotesDAO.persist(Notes.UUID_1, Person.USERNAME_1);
    final List<String> notesUUIDs = recentNotesDAO.getAll(Person.USERNAME_1);
    assertEquals(1, notesUUIDs.size());
    assertTrue(notesUUIDs.contains(Notes.UUID_1));
  }

  @Test
  public void cleanup() {
    createNote("temp_2", Notes.UUID_2);
    createNote("temp_3", Notes.UUID_3);
    createNote("temp_4", Notes.UUID_4);
    createNote("temp_5", Notes.UUID_5);
    createNote("temp_6", Notes.UUID_6);

    recentNotesDAO.persist(Notes.UUID_1, Person.USERNAME_1);
    recentNotesDAO.persist(Notes.UUID_2, Person.USERNAME_1);
    recentNotesDAO.persist(Notes.UUID_3, Person.USERNAME_1);
    recentNotesDAO.persist(Notes.UUID_4, Person.USERNAME_1);
    recentNotesDAO.persist(Notes.UUID_5, Person.USERNAME_1);
    recentNotesDAO.persist(Notes.UUID_6, Person.USERNAME_1);

    List<String> recentNotesUUIDs = recentNotesDAO.getAll(Person.USERNAME_1);
    assertEquals(6, recentNotesUUIDs.size());

    recentNotesDAO.cleanup();
    recentNotesUUIDs = recentNotesDAO.getAll(Person.USERNAME_1);
    assertEquals(5, recentNotesUUIDs.size());
    assertFalse(recentNotesUUIDs.contains(Notes.UUID_1));
  }

  @Test
  public void getAll() {
    createNote("temp_2", Notes.UUID_2);
    createNote("temp_3", Notes.UUID_3);
    createNote("temp_4", Notes.UUID_4);
    createNote("temp_5", Notes.UUID_5);
    createNote("temp_6", Notes.UUID_6);

    recentNotesDAO.persist(Notes.UUID_1, Person.USERNAME_1);
    recentNotesDAO.persist(Notes.UUID_2, Person.USERNAME_1);
    recentNotesDAO.persist(Notes.UUID_3, Person.USERNAME_1);
    recentNotesDAO.persist(Notes.UUID_4, Person.USERNAME_2);
    recentNotesDAO.persist(Notes.UUID_5, Person.USERNAME_2);
    recentNotesDAO.persist(Notes.UUID_6, Person.USERNAME_3);

    List<String> recentNoteUUIDs = recentNotesDAO.getAll(Person.USERNAME_1);
    assertEquals(3, recentNoteUUIDs.size());
    assertTrue(recentNoteUUIDs.contains(Notes.UUID_1));
    assertTrue(recentNoteUUIDs.contains(Notes.UUID_2));
    assertTrue(recentNoteUUIDs.contains(Notes.UUID_3));

    recentNoteUUIDs = recentNotesDAO.getAll(Person.USERNAME_2);
    assertEquals(2, recentNoteUUIDs.size());
    assertTrue(recentNoteUUIDs.contains(Notes.UUID_4));
    assertTrue(recentNoteUUIDs.contains(Notes.UUID_5));

    recentNoteUUIDs = recentNotesDAO.getAll(Person.USERNAME_3);
    assertEquals(1, recentNoteUUIDs.size());
    assertTrue(recentNoteUUIDs.contains(Notes.UUID_6));
  }

  @Test
  public void deleteNote() {
    recentNotesDAO.persist(Notes.UUID_1, Person.USERNAME_1);
    new NoteDAO(jdbcTemplate).remove(getNote(Notes.UUID_1));
    assertTrue(recentNotesDAO.getAll(Person.USERNAME_1).isEmpty());
  }
}
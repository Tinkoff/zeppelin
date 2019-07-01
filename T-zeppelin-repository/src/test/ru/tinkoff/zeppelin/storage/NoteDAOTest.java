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
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.dao.DuplicateKeyException;
import ru.tinkoff.zeppelin.core.notebook.Note;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class NoteDAOTest extends AbstractTest {

  private static NoteDAO noteDao;

  @BeforeClass
  public static void init() {
    noteDao = new NoteDAO(jdbcTemplate);
  }

  @After
  public void afterEach() {
    deleteNote(Notes.UUID_1);
    deleteNote(Notes.UUID_2);
    deleteNote(Notes.UUID_3);
  }

  @Test(expected = DuplicateKeyException.class)
  public void persist() {
    final Note note = new Note("temp_1");
    note.setUuid(Notes.UUID_1);
    note.getOwners().add(Person.USERNAME_1);
    note.getReaders().add(Person.USERNAME_2);
    note.getWriters().add(Person.USERNAME_3);
    note.getRunners().add(Person.USERNAME_4);
    noteDao.persist(note);
    assertNotNull(noteDao.get(Notes.UUID_1));

    noteDao.persist(note);
  }

  @Test
  public void update() {
    final Note note = new Note("temp_1");
    note.setUuid(Notes.UUID_1);
    noteDao.persist(note);
    note.setPath("new_path");
    noteDao.update(note);
    assertEquals(note, noteDao.get(note.getUuid()));
  }

  @Test
  public void getByUUID() {
    final Note note = new Note("temp_1");
    note.setUuid(Notes.UUID_1);
    noteDao.persist(note);

    final Note noteDB = noteDao.get(note.getUuid());
    assertNotNull(noteDB);
    assertEquals(note, noteDB);
  }

  @Test
  public void getById() {
    final Note note = new Note("temp_1");
    note.setUuid(Notes.UUID_1);
    noteDao.persist(note);

    final Note noteDB = noteDao.get(note.getId());
    assertNotNull(noteDB);
    assertEquals(note, noteDB);
  }

  @Test
  public void remove() {
    final Note note = new Note("temp_1");
    note.setUuid(Notes.UUID_1);
    noteDao.persist(note);
    noteDao.remove(note);
    assertNull(noteDao.get(note.getId()));
  }

  @Test
  public void getAllNotes() {
    createNote("temp_1", Notes.UUID_1);
    createNote("temp_2", Notes.UUID_2);
    createNote("temp_3", Notes.UUID_3);

    final List<Note> allNotes = noteDao.getAllNotes();
    assertEquals(3, allNotes.size());
    final List<String> notePaths = allNotes.stream().map(Note::getPath).collect(Collectors.toList());
    assertTrue(notePaths.contains("temp_1"));
    assertTrue(notePaths.contains("temp_2"));
    assertTrue(notePaths.contains("temp_3"));
  }
}
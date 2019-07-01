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
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.NoteRevision;

import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;

public class NoteRevisionDAOTest extends AbstractTest {

  private static NoteRevisionDAO noteRevisionDAO;

  @BeforeClass
  public static void init() {
    noteRevisionDAO = new NoteRevisionDAO(jdbcTemplate);
  }

  @Before
  public void forEach() {
    createNote("temp_1", Notes.UUID_1);
  }

  @After
  public void afterEach() {
    deleteNote(Notes.UUID_1);
  }

  @Test
  public void createRevision() {
    final NoteRevision revision = noteRevisionDAO.createRevision(getNote(Notes.UUID_1), "Rev");
    assertNotNull(revision);
  }

  @Test
  public void getRevisions() {
    final Note note = getNote(Notes.UUID_1);
    final NoteRevision revision1 = noteRevisionDAO.createRevision(note, "Rev1");
    final NoteRevision revision2 = noteRevisionDAO.createRevision(note, "Rev2");
    final List<NoteRevision> revisions = noteRevisionDAO.getRevisions(note.getId());
    revisions.sort(Comparator.comparing(NoteRevision::getDate));

    assertEquals(2, revisions.size());
    assertEquals(revision1.getMessage(), "Rev1");
    assertEquals(revision2.getMessage(), "Rev2");
  }

  @Test
  public void deleteNote() {
    final Note note = getNote(Notes.UUID_1);
    noteRevisionDAO.createRevision(note, "Rev1");
    deleteNote(note.getUuid());
    assertTrue(noteRevisionDAO.getRevisions(note.getId()).isEmpty());
  }
}
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

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FavoriteNotesDAOTest extends AbstractTest {

  private static FavoriteNotesDAO favoriteNotesDAO;

  @BeforeClass
  public static void init() {
    favoriteNotesDAO = new FavoriteNotesDAO(jdbcTemplate);
  }

  @Before
  public void forEach() {
    // create temp notes
    createNote("note_1", Notes.UUID_1);
    createNote("note_2", Notes.UUID_2);
  }

  @After
  public void afterEach() {
    // remove temp notes
    deleteNote(Notes.UUID_1);
    deleteNote(Notes.UUID_2);
  }

  @Test
  public void checkAddFavorite() {
    // USER_1 add NOTE_1
    favoriteNotesDAO.persist(Notes.UUID_1, Person.USERNAME_1);
    List<String> favoriteNotesUuidList = favoriteNotesDAO.getAll(Person.USERNAME_1);
    assertEquals(1, favoriteNotesUuidList.size());
    assertEquals(favoriteNotesUuidList.get(0), Notes.UUID_1);

    // USER_1 add NOTE_2
    favoriteNotesDAO.persist(Notes.UUID_2, Person.USERNAME_1);
    favoriteNotesUuidList = favoriteNotesDAO.getAll(Person.USERNAME_1);
    assertEquals(2, favoriteNotesUuidList.size());
    assertTrue(favoriteNotesUuidList.contains(Notes.UUID_1));
    assertTrue(favoriteNotesUuidList.contains(Notes.UUID_2));

    // USER_2 add NOTE_1
    favoriteNotesDAO.persist(Notes.UUID_1, Person.USERNAME_2);
    favoriteNotesUuidList = favoriteNotesDAO.getAll(Person.USERNAME_2);
    assertEquals(1, favoriteNotesUuidList.size());
    assertTrue(favoriteNotesUuidList.contains(Notes.UUID_1));
    favoriteNotesUuidList = favoriteNotesDAO.getAll(Person.USERNAME_1);
    assertEquals(2, favoriteNotesUuidList.size());
    assertTrue(favoriteNotesUuidList.contains(Notes.UUID_1));
    assertTrue(favoriteNotesUuidList.contains(Notes.UUID_2));
  }

  @Test
  public void checkRemoveFavorite() {
    favoriteNotesDAO.persist(Notes.UUID_1, Person.USERNAME_1);
    favoriteNotesDAO.persist(Notes.UUID_2, Person.USERNAME_1);
    favoriteNotesDAO.persist(Notes.UUID_1, Person.USERNAME_2);
    favoriteNotesDAO.persist(Notes.UUID_2, Person.USERNAME_2);

    // USER_1 remove NOTE_1
    favoriteNotesDAO.remove(Notes.UUID_1, Person.USERNAME_1);
    List<String> favoriteNotesUuidList = favoriteNotesDAO.getAll(Person.USERNAME_1);
    assertEquals(1, favoriteNotesUuidList.size());
    assertTrue(favoriteNotesUuidList.contains(Notes.UUID_2));
    favoriteNotesUuidList = favoriteNotesDAO.getAll(Person.USERNAME_2);
    assertEquals(2, favoriteNotesUuidList.size());
    assertTrue(favoriteNotesUuidList.contains(Notes.UUID_1));
    assertTrue(favoriteNotesUuidList.contains(Notes.UUID_2));

    // USER_1 remove NOTE_2
    favoriteNotesDAO.remove(Notes.UUID_2, Person.USERNAME_1);
    favoriteNotesUuidList = favoriteNotesDAO.getAll(Person.USERNAME_1);
    assertEquals(0, favoriteNotesUuidList.size());
    favoriteNotesUuidList = favoriteNotesDAO.getAll(Person.USERNAME_2);
    assertEquals(2, favoriteNotesUuidList.size());
    assertTrue(favoriteNotesUuidList.contains(Notes.UUID_1));
    assertTrue(favoriteNotesUuidList.contains(Notes.UUID_2));
  }

  @Test
  public void checkRemoveNote() {
    favoriteNotesDAO.persist(Notes.UUID_1, Person.USERNAME_1);
    deleteNote(Notes.UUID_1);
    final List<String> favoriteNotesUuidList = favoriteNotesDAO.getAll(Person.USERNAME_1);
    assertEquals(0, favoriteNotesUuidList.size());
  }
}
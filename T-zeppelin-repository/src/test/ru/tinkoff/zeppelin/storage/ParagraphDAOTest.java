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
import ru.tinkoff.zeppelin.core.notebook.NoteRevision;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class ParagraphDAOTest extends AbstractTest {

  private static ParagraphDAO paragraphDAO;

  @BeforeClass
  public static void init() {
    paragraphDAO = new ParagraphDAO(jdbcTemplate);
  }

  @Before
  public void forEach() {
    createNote("temp_1", Notes.UUID_1);
    createNote("temp_2", Notes.UUID_2);
    createNote("temp_3", Notes.UUID_3);
    createNote("temp_4", Notes.UUID_4);
  }

  @After
  public void afterEach() {
    deleteNote(Notes.UUID_1);
    deleteNote(Notes.UUID_2);
    deleteNote(Notes.UUID_3);
    deleteNote(Notes.UUID_4);
  }

  static Paragraph getTestParagraph(final String noteUUID) {
    final Note note = getNote(noteUUID);
    final Paragraph paragraph = new Paragraph();
    final int random = new Random().nextInt(100);

    paragraph.setNoteId(note.getId());
    paragraph.setTitle("Title Text" + random);
    paragraph.setText("Paragraph Text" + random);
    paragraph.setShebang("Shebang" + random);
    paragraph.setCreated(LocalDateTime.now().minusDays(random - 1));
    paragraph.setUpdated(LocalDateTime.now().minusDays(random));
    paragraph.setPosition(random);
    return paragraph;
  }

  @Test
  public void persist() {
    final Paragraph paragraph = getTestParagraph(Notes.UUID_1);
    paragraphDAO.persist(paragraph);

    final List<Paragraph> paragraphs = paragraphDAO.getByNoteId(getNote(Notes.UUID_1).getId());
    assertEquals(1, paragraphs.size());
    assertEquals(paragraph, paragraphs.get(0));
  }

  @Test
  public void update() {
    final Paragraph paragraph = getTestParagraph(Notes.UUID_1);
    paragraphDAO.persist(paragraph);

    paragraph.setTitle("Title Text NEW");
    paragraph.setText("Paragraph Text NEW");
    paragraph.setShebang("Shebang NEW");
    paragraph.setCreated(LocalDateTime.now().minusHours(5));
    paragraph.setUpdated(LocalDateTime.now().minusHours(1));
    paragraph.setPosition(1);

    paragraphDAO.update(paragraph);

    final List<Paragraph> paragraphs = paragraphDAO.getByNoteId(getNote(Notes.UUID_1).getId());
    assertEquals(1, paragraphs.size());
    assertEquals(paragraph, paragraphs.get(0));
  }

  @Test
  public void getAll() {

    final List<Paragraph> paragraphs = Arrays.asList(
        getTestParagraph(Notes.UUID_1), getTestParagraph(Notes.UUID_1),
        getTestParagraph(Notes.UUID_2), getTestParagraph(Notes.UUID_2),
        getTestParagraph(Notes.UUID_3), getTestParagraph(Notes.UUID_3)
    );

    final Comparator<Paragraph> paragraphComparator = Comparator.comparing(Paragraph::getNoteId)
        .thenComparingInt(Paragraph::getPosition);

    paragraphs.sort(paragraphComparator);
    paragraphs.forEach(paragraphDAO::persist);

    final List<Paragraph> dbParagraphs = paragraphDAO.getAll();
    dbParagraphs.sort(paragraphComparator);

    assertEquals(paragraphs.size(), dbParagraphs.size());
    assertArrayEquals(paragraphs.toArray(), dbParagraphs.toArray());

    deleteNote(Notes.UUID_2);
    deleteNote(Notes.UUID_3);
  }

  @Test
  public void getById() {
    final Paragraph paragraph = getTestParagraph(Notes.UUID_1);
    paragraphDAO.persist(paragraph);

    assertEquals(paragraph, paragraphDAO.get(paragraph.getId()));
  }

  @Test
  public void getByUUID() {
    final Paragraph paragraph = getTestParagraph(Notes.UUID_1);
    paragraphDAO.persist(paragraph);

    assertEquals(paragraph, paragraphDAO.get(paragraph.getUuid()));
  }

  @Test
  public void remove() {
    final Paragraph paragraph = getTestParagraph(Notes.UUID_1);
    paragraphDAO.persist(paragraph);
    paragraphDAO.remove(paragraph);
    assertNull(paragraphDAO.get(paragraph.getId()));
  }

  @Test
  public void getByNoteId() {
    final List<Paragraph> paragraphs = Arrays.asList(
        getTestParagraph(Notes.UUID_1),
        getTestParagraph(Notes.UUID_1)
    );
    paragraphs.sort(Comparator.comparingInt(Paragraph::getPosition));
    paragraphs.forEach(paragraphDAO::persist);

    paragraphDAO.persist(getTestParagraph(Notes.UUID_2));

    final List<Paragraph> dbParagraphs = paragraphDAO.getByNoteId(getNote(Notes.UUID_1).getId());
    dbParagraphs.sort(Comparator.comparingInt(Paragraph::getPosition));

    assertEquals(paragraphs.size(), dbParagraphs.size());
    assertArrayEquals(paragraphs.toArray(), dbParagraphs.toArray());
  }

  @Test
  public void getByRevisionId() {
    final Note note = getNote(Notes.UUID_1);

    final Paragraph[] paragraphs = new Paragraph[3];

    paragraphs[0] = getTestParagraph(note.getUuid());
    paragraphs[0].setText("Paragraph 0 Revision 0");
    paragraphs[0].setPosition(0);
    paragraphDAO.persist(paragraphs[0]);

    final NoteRevision revision0 = persistRevision(note, "Rev 0 message");

    paragraphs[0].setText("Paragraph 0 Revision 1");
    paragraphs[1] = getTestParagraph(note.getUuid());
    paragraphs[1].setText("Paragraph 1 Revision 1");
    paragraphs[1].setPosition(1);
    paragraphDAO.update(paragraphs[0]);
    paragraphDAO.persist(paragraphs[1]);

    final NoteRevision revision1 = persistRevision(note, "Rev 1 message");

    paragraphs[0].setText("Paragraph 0 Revision HEAD");
    paragraphs[1].setText("Paragraph 1 Revision HEAD");
    paragraphs[2] = getTestParagraph(note.getUuid());
    paragraphs[2].setText("Paragraph 2 Revision HEAD");
    paragraphs[2].setPosition(2);
    paragraphDAO.update(paragraphs[0]);
    paragraphDAO.update(paragraphs[1]);
    paragraphDAO.persist(paragraphs[2]);

    List<Paragraph> revParagraphs = paragraphDAO.getByRevisionId(revision0.getId());
    assertEquals(1, revParagraphs.size());
    assertEquals("Paragraph 0 Revision 0", revParagraphs.get(0).getText());

    revParagraphs = paragraphDAO.getByRevisionId(revision1.getId());
    revParagraphs.sort(Comparator.comparingInt(Paragraph::getPosition));
    assertEquals(2, revParagraphs.size());
    assertEquals("Paragraph 0 Revision 1", revParagraphs.get(0).getText());
    assertEquals("Paragraph 1 Revision 1", revParagraphs.get(1).getText());
  }

  private NoteRevision persistRevision(final Note note, final String message) {
    final NoteRevisionDAO revisionDAO = new NoteRevisionDAO(jdbcTemplate);
    final NoteRevision revision = revisionDAO.createRevision(note, message);
    final List<Paragraph> paragraphs = paragraphDAO.getByNoteId(note.getId());
    paragraphs.stream()
        .peek(p -> p.setRevisionId(revision.getId()))
        .peek(p -> p.setJobId(null))
        .forEach(paragraphDAO::persist);

    return revision;
  }
}




































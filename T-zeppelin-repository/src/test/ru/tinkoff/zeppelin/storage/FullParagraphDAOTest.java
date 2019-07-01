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
import ru.tinkoff.zeppelin.core.externalDTO.ParagraphDTO;
import ru.tinkoff.zeppelin.core.notebook.Job;
import ru.tinkoff.zeppelin.core.notebook.JobBatch;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;

import java.time.LocalDateTime;

import static org.junit.Assert.*;

public class FullParagraphDAOTest extends AbstractTest {


  private static FullParagraphDAO fullParagraphDAO;

  private Note note;
  private Paragraph paragraph;
  private JobBatch jobBatch;
  private static ParagraphDAO paragraphDAO;
  private static JobDAO jobDAO;

  @BeforeClass
  public static void init() {
    fullParagraphDAO = new FullParagraphDAO(jdbcTemplate);
  }

  @AfterClass
  public static void finish() {
    deleteNote(Notes.UUID_1);
  }

  @Before
  public void forEach() {
    paragraphDAO = new ParagraphDAO(jdbcTemplate);
    final JobBatchDAO jobBatchDAO = new JobBatchDAO(jdbcTemplate);
    jobDAO = new JobDAO(jdbcTemplate);
    // create Note
    note = createNote("temp_1", Notes.UUID_1);

    // create Paragraph
    paragraph = ParagraphDAOTest.getTestParagraph(note.getUuid());
    paragraph.setPosition(0);
    paragraphDAO.persist(paragraph);

    // create JobBatch
    jobBatch = JobBatchDAOTest.getTestBatch(note);
    jobBatchDAO.persist(jobBatch);
  }

  @After
  public void afterEach() {
    deleteNote(Notes.UUID_1);
  }

  private Job getTestJob() {
    final Job job = new Job();
    job.setBatchId(jobBatch.getId());
    job.setNoteId(note.getId());
    job.setParagraphId(paragraph.getId());
    job.setStatus(Job.Status.DONE);
    job.setShebang("python");
    job.setUsername(Person.USERNAME_1);
    job.setEndedAt(LocalDateTime.now());
    job.setStartedAt(LocalDateTime.now().minusMinutes(10));
    job.setPriority(4);
    return job;
  }

  @Test
  public void getById() {
    final Job job = getTestJob();
    jobDAO.persist(job);

    paragraph.setJobId(job.getId());
    paragraphDAO.update(paragraph);

    final ParagraphDTO dbFullParagraph = fullParagraphDAO.getById(paragraph.getId());
    assertEquals(dbFullParagraph.getJobId(), job.getId());
    assertEquals(dbFullParagraph.getDatabaseId(), (long) paragraph.getId());
    assertEquals(dbFullParagraph.getId(), paragraph.getUuid());
    assertEquals(dbFullParagraph.getTitle(), paragraph.getTitle());
    assertEquals(dbFullParagraph.getText(), paragraph.getText());
    assertEquals(dbFullParagraph.getUser(), job.getUsername());
    assertEquals(dbFullParagraph.getShebang(), paragraph.getShebang());
    assertEquals(dbFullParagraph.getCreated(), paragraph.getCreated());
    assertEquals(dbFullParagraph.getUpdated(), paragraph.getUpdated());
    assertEquals(dbFullParagraph.getStartedAt(), job.getStartedAt());
    assertEquals(dbFullParagraph.getEndedAt(), job.getEndedAt());
    assertEquals(dbFullParagraph.getStatus(), job.getStatus().name());
    assertEquals(dbFullParagraph.getPosition(), (int) paragraph.getPosition());
  }
}
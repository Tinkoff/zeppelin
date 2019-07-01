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
import ru.tinkoff.zeppelin.core.notebook.*;

import java.time.LocalDateTime;

import static org.junit.Assert.*;

public class JobPayloadDAOTest extends AbstractTest {

  private static JobPayloadDAO jobPayloadDAO;

  private Job job;

  @BeforeClass
  public static void init() {
     jobPayloadDAO = new JobPayloadDAO(jdbcTemplate);
  }

  @Before
  public void forEach() {
    // create Note
    final Note note = createNote("temp_1", Notes.UUID_1);

    // create Paragraph
    final Paragraph paragraph = new Paragraph();
    paragraph.setTitle("title");
    paragraph.setText("text");
    paragraph.setPosition(0);
    paragraph.setCreated(LocalDateTime.now());
    paragraph.setUpdated(LocalDateTime.now());
    paragraph.setNoteId(note.getId());
    new ParagraphDAO(jdbcTemplate).persist(paragraph);

    // create JobBatch
    final JobBatch jobBatch = new JobBatch();
    jobBatch.setNoteId(note.getId());
    jobBatch.setStatus(JobBatch.Status.DONE);
    jobBatch.setCreatedAt(LocalDateTime.now());
    new JobBatchDAO(jdbcTemplate).persist(jobBatch);

    // create Job
    job = new Job();
    job.setBatchId(jobBatch.getId());
    job.setNoteId(note.getId());
    job.setParagraphId(paragraph.getId());
    job.setStatus(Job.Status.DONE);
    job.setShebang("python");
    job.setUsername(Person.USERNAME_1);
    new JobDAO(jdbcTemplate).persist(job);
  }

  @After
  public void afterEach() {
    deleteNote(Notes.UUID_1);
  }

  @Test
  public void persist() {
    final JobPayload jobPayload = new JobPayload();
    jobPayload.setPayload("print(1)");
    jobPayload.setJobId(job.getId());
    jobPayloadDAO.persist(jobPayload);

    assertNotNull(jobPayload.getId());
  }

  @Test
  public void get() {
    final JobPayload jobPayload = new JobPayload();
    jobPayload.setPayload("print(1)");
    jobPayload.setJobId(job.getId());
    jobPayloadDAO.persist(jobPayload);

    final JobPayload dbJobPayload = jobPayloadDAO.get(jobPayload.getId());
    assertNotNull(dbJobPayload);
    assertEquals(jobPayload.getPayload(), dbJobPayload.getPayload());
    assertEquals(jobPayload.getJobId(), dbJobPayload.getJobId());
  }

  @Test
  public void getByJobId() {
    final JobPayload jobPayload = new JobPayload();
    jobPayload.setPayload("print(1)");
    jobPayload.setJobId(job.getId());
    jobPayloadDAO.persist(jobPayload);

    final JobPayload dbJobPayload = jobPayloadDAO.getByJobId(job.getId());
    assertNotNull(dbJobPayload);
    assertEquals(jobPayload.getPayload(), dbJobPayload.getPayload());
    assertEquals(jobPayload.getJobId(), dbJobPayload.getJobId());
  }
}
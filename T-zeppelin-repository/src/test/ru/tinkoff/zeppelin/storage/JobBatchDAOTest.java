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
import ru.tinkoff.zeppelin.core.notebook.JobBatch;
import ru.tinkoff.zeppelin.core.notebook.Note;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.Assert.*;

public class JobBatchDAOTest extends AbstractTest {

  private static JobBatchDAO jobBatchDAO;

  private Note note;

  @BeforeClass
  public static void init() {
    jobBatchDAO = new JobBatchDAO(jdbcTemplate);
  }

  @Before
  public void forEach() {
    note = createNote("temp_1", Notes.UUID_1);
  }

  @After
  public void afterEach() {
    deleteNote(Notes.UUID_1);
  }

  static JobBatch getTestBatch(final Note note) {
    final JobBatch jobBatch = new JobBatch();
    jobBatch.setNoteId(note.getId());
    jobBatch.setStatus(JobBatch.Status.DONE);
    jobBatch.setCreatedAt(LocalDateTime.now());
    return jobBatch;
  }

  @Test
  public void persist() {
    final JobBatch jobBatch = getTestBatch(note);
    jobBatchDAO.persist(jobBatch);
    assertNotEquals(0, jobBatch.getId());
  }

  @Test
  public void update() {
    final JobBatch jobBatch = getTestBatch(note);
    jobBatchDAO.persist(jobBatch);
    jobBatch.setStatus(JobBatch.Status.RUNNING);
    jobBatch.setStartedAt(LocalDateTime.now());
    jobBatch.setEndedAt(LocalDateTime.now().plusMinutes(1));

    jobBatchDAO.update(jobBatch);
    final JobBatch dbJobBatch = jobBatchDAO.get(jobBatch.getId());

    assertEquals(jobBatch, dbJobBatch);
  }

  @Test
  public void delete() {
    final JobBatch jobBatch = getTestBatch(note);
    jobBatchDAO.persist(jobBatch);
    jobBatchDAO.delete(jobBatch.getId());
    assertNull(jobBatchDAO.get(jobBatch.getId()));
  }

  @Test
  public void get() {
    final JobBatch jobBatch = getTestBatch(note);
    jobBatchDAO.persist(jobBatch);
    jobBatch.setStatus(JobBatch.Status.RUNNING);
    jobBatch.setStartedAt(LocalDateTime.now());
    jobBatch.setEndedAt(LocalDateTime.now().plusMinutes(1));

    jobBatchDAO.persist(jobBatch);
    final JobBatch dbJobBatch = jobBatchDAO.get(jobBatch.getId());
    assertEquals(jobBatch, dbJobBatch);
  }

  @Test
  public void getAborting() {
    final JobBatch jobBatchRunning = getTestBatch(note);
    final JobBatch jobBatchAborted = getTestBatch(note);
    jobBatchRunning.setStatus(JobBatch.Status.RUNNING);
    jobBatchAborted.setStatus(JobBatch.Status.ABORTING);
    jobBatchDAO.persist(jobBatchRunning);
    jobBatchDAO.persist(jobBatchAborted);

    final List<JobBatch> abortingJobBatch = jobBatchDAO.getAborting();
    assertEquals(1, abortingJobBatch.size());
    assertTrue(abortingJobBatch.contains(jobBatchAborted));
  }
}
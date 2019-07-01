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
import ru.tinkoff.zeppelin.core.notebook.Job;
import ru.tinkoff.zeppelin.core.notebook.JobBatch;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;

import java.time.LocalDateTime;
import java.util.*;

import static org.junit.Assert.*;

public class JobDAOTest extends AbstractTest {

  private static JobDAO jobDAO;

  private static Note note;
  private static Paragraph paragraph;
  private static JobBatch jobBatch;
  private static ParagraphDAO paragraphDAO;
  private static JobBatchDAO jobBatchDAO;

  @BeforeClass
  public static void init() {
    jobDAO = new JobDAO(jdbcTemplate);
  }

  @Before
  public void forEach() {
    paragraphDAO = new ParagraphDAO(jdbcTemplate);
    jobBatchDAO = new JobBatchDAO(jdbcTemplate);
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
    deleteNote(Notes.UUID_2);
  }

  static Job getTestJob() {
    final Job job = new Job();
    job.setBatchId(jobBatch.getId());
    job.setNoteId(note.getId());
    job.setParagraphId(paragraph.getId());
    job.setStatus(Job.Status.DONE);
    job.setShebang("python");
    job.setUsername(Person.USERNAME_1);
    return job;
  }

  @Test
  public void persist() {
    final Job job = getTestJob();
    jobDAO.persist(job);
    assertNotEquals(0, job.getId());
  }

  @Test
  public void update() {
    final Job job = getTestJob();
    jobDAO.persist(job);

    job.setUsername(Person.USERNAME_2);
    job.setShebang("jdbc");
    job.setStatus(Job.Status.ABORTING);
    job.setEndedAt(LocalDateTime.now());
    jobDAO.update(job);

    final Job dbJob = jobDAO.get(job.getId());
    assertEquals(job, dbJob);
  }

  @Test
  public void get() {
    final Job job = getTestJob();
    jobDAO.persist(job);
    assertEquals(job, jobDAO.get(job.getId()));
  }

  @Test
  public void getByInterpreterJobUUID() {
    final Job job = getTestJob();
    job.setInterpreterJobUUID("I34SH2FI2S3EFI4J2SF3I54");
    jobDAO.persist(job);
    jobDAO.persist(getTestJob());
    assertEquals(job, jobDAO.getByInterpreterJobUUID(job.getInterpreterJobUUID()));
  }

  @Test
  public void loadNextPending() {
    // prepare
    final Note note = createNote("temp_2", Notes.UUID_2);
    final List<Job> jobs = new ArrayList<>(Arrays.asList(getTestJob(), getTestJob(), getTestJob()));
    final List<Paragraph> paragraphs = new ArrayList<>(
        Arrays.asList(
            ParagraphDAOTest.getTestParagraph(note.getUuid()),
            ParagraphDAOTest.getTestParagraph(note.getUuid()),
            ParagraphDAOTest.getTestParagraph(note.getUuid())
        )
    );
    paragraphs.get(0).setPosition(0);
    paragraphs.get(1).setPosition(1);
    paragraphs.get(2).setPosition(2);
    paragraphs.forEach(paragraphDAO::persist);

    jobs.get(0).setParagraphId(paragraphs.get(0).getId());
    jobs.get(1).setParagraphId(paragraphs.get(1).getId());
    jobs.get(2).setParagraphId(paragraphs.get(2).getId());

    final JobBatch jobBatch = JobBatchDAOTest.getTestBatch(note);
    jobBatchDAO.persist(jobBatch);
    jobs.forEach(j -> j.setBatchId(jobBatch.getId()));
    jobs.forEach(jobDAO::persist);

    // tests
    LinkedList<Job> dbJobs = jobDAO.loadNextPending();
    assertTrue(dbJobs.isEmpty());

    jobBatch.setStatus(JobBatch.Status.RUNNING);
    jobBatchDAO.update(jobBatch);
    jobs.get(0).setStatus(Job.Status.PENDING);
    jobs.get(1).setStatus(Job.Status.PENDING);
    jobs.get(2).setStatus(Job.Status.PENDING);
    jobs.forEach(jobDAO::update);
    dbJobs = jobDAO.loadNextPending();
    assertEquals(1, dbJobs.size());
    assertTrue(dbJobs.contains(jobs.get(0)));

    jobs.get(0).setStatus(Job.Status.DONE);
    jobDAO.update(jobs.get(0));
    dbJobs = jobDAO.loadNextPending();
    assertEquals(1, dbJobs.size());
    assertTrue(dbJobs.contains(jobs.get(1)));

    jobs.get(1).setStatus(Job.Status.ERROR);
    jobDAO.update(jobs.get(1));
    dbJobs = jobDAO.loadNextPending();
    assertTrue(dbJobs.isEmpty());
  }

  @Test
  public void loadByBatch() {
    final Job job = getTestJob();
    jobDAO.persist(job);
    final List<Job> jobs = jobDAO.loadByBatch(jobBatch.getId());
    assertEquals(1, jobs.size());
    assertEquals(job, jobs.get(0));
  }

  @Test
  public void loadNextCancelling() {
    final Job job = getTestJob();
    job.setStatus(Job.Status.RUNNING);
    jobDAO.persist(job);

    jobBatch.setStatus(JobBatch.Status.ABORTING);
    jobBatchDAO.update(jobBatch);

    final List<Job> cancellingJobs = jobDAO.loadNextCancelling();
    assertEquals(1, cancellingJobs.size());
    assertEquals(job, cancellingJobs.get(0));
  }

  @Test
  public void restoreState() {
    final String INTERPRETER_PROCESS_UUID = "INTERPRETER_PROCESS_UUID";
    final Job job = getTestJob();
    job.setStatus(Job.Status.RUNNING);
    job.setInterpreterProcessUUID(INTERPRETER_PROCESS_UUID);
    jobDAO.persist(job);

    jobDAO.restoreState(Collections.singletonList("NO_" + INTERPRETER_PROCESS_UUID));
    assertEquals(Job.Status.PENDING, jobDAO.get(job.getId()).getStatus());
  }
}
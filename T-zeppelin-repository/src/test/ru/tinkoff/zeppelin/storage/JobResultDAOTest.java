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
import ru.tinkoff.zeppelin.core.notebook.JobResult;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class JobResultDAOTest extends AbstractTest {

  private static JobResultDAO jobResultDAO;
  private static JobDAO jobDAO;

  private static Job job;

  @BeforeClass
  public static void init() {
    jobResultDAO = new JobResultDAO(jdbcTemplate);
    jobDAO = new JobDAO(jdbcTemplate);
  }

  @Before
  public void forEach() {
    new JobDAOTest().forEach();
    job = JobDAOTest.getTestJob();
    jobDAO.persist(job);
  }

  @After
  public void afterEach() {
    new JobDAOTest().afterEach();
    jobResults.forEach(j -> jobResultDAO.delete(j.getId()));
    jobResults.clear();
  }

  private static final List<JobResult> jobResults = new ArrayList<>();

  private static JobResult getTestJobResult() {
    final JobResult jobResult = new JobResult();
    jobResult.setId(-100L);
    jobResult.setResult("result");
    jobResult.setCreatedAt(LocalDateTime.now());
    jobResult.setJobId(job.getId());
    jobResult.setType("JOB_RESULT_TYPE");
    jobResults.add(jobResult);
    return jobResult;
  }

  @Test
  public void persist() {
    final JobResult jobResult = getTestJobResult();
    jobResultDAO.persist(jobResult);
    assertNotEquals(-100L, (long) jobResult.getId());
  }

  @Test
  public void get() {
    final JobResult jobResult = getTestJobResult();
    jobResultDAO.persist(jobResult);

    final JobResult dbJobResult = jobResultDAO.get(jobResult.getId());
    assertEquals(jobResult, dbJobResult);
  }

  @Test
  public void update() {
    final JobResult jobResult = getTestJobResult();
    jobResultDAO.persist(jobResult);

    jobResult.setCreatedAt(LocalDateTime.now());
    jobResult.setResult("New_Result");
    jobResultDAO.update(jobResult);

    final JobResult dbJobResult = jobResultDAO.get(jobResult.getId());
    assertEquals(jobResult, dbJobResult);
  }

  @Test
  public void getByJobId() {
    final List<JobResult> jobResults = Arrays.asList(
        getTestJobResult(),
        getTestJobResult(),
        getTestJobResult()
    );

    jobResults.get(0).setResult("Result_0");
    jobResults.get(1).setResult("Result_1");
    jobResults.get(2).setResult("Result_2");
    jobResults.forEach(jobResultDAO::persist);

    final List<JobResult> dbJobResults = jobResultDAO.getByJobId(job.getId());
    assertEquals(jobResults.size(), dbJobResults.size());
    assertEquals(jobResults.get(0), dbJobResults.get(0));
    assertEquals(jobResults.get(1), dbJobResults.get(1));
    assertEquals(jobResults.get(2), dbJobResults.get(2));
  }

  @Test
  public void delete() {
    final JobResult jobResult = getTestJobResult();
    jobResultDAO.persist(jobResult);

    jobResultDAO.delete(jobResult.getId());
    assertNull(jobResultDAO.get(jobResult.getId()));
  }
}
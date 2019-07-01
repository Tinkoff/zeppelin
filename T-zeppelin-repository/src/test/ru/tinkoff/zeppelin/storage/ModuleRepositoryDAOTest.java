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

import org.apache.zeppelin.Repository;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;

public class ModuleRepositoryDAOTest extends AbstractTest {

  private static ModuleRepositoryDAO moduleRepositoryDAO;

  @BeforeClass
  public static void init() {
    moduleRepositoryDAO = new ModuleRepositoryDAO(jdbcTemplate);
  }

  private static final List<Repository> repositoriesList = new ArrayList<>();

  private static Repository getTestRepository() {
    final Repository repository = new Repository(
        false,
        "id",
        "https://github.com/TinkoffCreditSystems/zeppelin",
        Person.USERNAME_1,
        "password",
        Repository.ProxyProtocol.HTTPS,
        "proxyHost",
        8080,
        "proxyLogin",
        "proxyPassword"
    );
    repositoriesList.add(repository);
    return repository;
  }

  @After
  public void afterEach() {
    repositoriesList.forEach(r -> moduleRepositoryDAO.delete(r.getId()));
    repositoriesList.clear();
  }

  @Test
  public void persist() {
    final Repository testRepository = getTestRepository();
    moduleRepositoryDAO.persist(testRepository);
    assertNotNull(moduleRepositoryDAO.getById(testRepository.getId()));
  }

  @Test
  public void getById() {
    final Repository testRepository = getTestRepository();
    moduleRepositoryDAO.persist(testRepository);

    final Repository dbRepository = moduleRepositoryDAO.getById(testRepository.getId());
    assertEquals(dbRepository, testRepository);
  }

  @Test
  public void getAll() {
    final List<Repository> repositories = Arrays.asList(
        getTestRepository(),
        getTestRepository(),
        getTestRepository()
    );

    repositories.get(0).setId("id0");
    repositories.get(0).setUsername(Person.USERNAME_1);
    repositories.get(0).setUrl("new_url_0");
    repositories.get(1).setId("id1");
    repositories.get(1).setUsername(Person.USERNAME_2);
    repositories.get(1).setUrl("new_url_1");
    repositories.get(2).setId("id2");
    repositories.get(2).setUsername(Person.USERNAME_3);
    repositories.get(2).setUrl("new_url_2");

    repositories.forEach(moduleRepositoryDAO::persist);

    final List<Repository> allRepositories = moduleRepositoryDAO.getAll();
    repositories.sort(Comparator.comparing(Repository::getId));
    allRepositories.sort(Comparator.comparing(Repository::getId));
    assertEquals(repositories.size(), allRepositories.size());
    assertEquals(repositories.get(0), allRepositories.get(0));
    assertEquals(repositories.get(1), allRepositories.get(1));
    assertEquals(repositories.get(2), allRepositories.get(2));
  }

  @Test
  public void delete() {
    final Repository testRepository = getTestRepository();
    moduleRepositoryDAO.persist(testRepository);

    moduleRepositoryDAO.delete(testRepository.getId());
    assertNull(moduleRepositoryDAO.getById(testRepository.getId()));
  }
}
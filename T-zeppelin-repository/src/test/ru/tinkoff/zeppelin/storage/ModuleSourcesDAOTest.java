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
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;

public class ModuleSourcesDAOTest extends AbstractTest {

  private static ModuleSourcesDAO moduleSourcesDAO;

  @BeforeClass
  public static void init() {
    moduleSourcesDAO = new ModuleSourcesDAO(jdbcTemplate);
  }

  @After
  public void afterEach() {
    sources.forEach(s -> moduleSourcesDAO.delete(s.getId()));
    sources.clear();
  }

  private static final List<ModuleSource> sources = new ArrayList<>();

  static ModuleSource getTestModuleSource() {
    final ModuleSource source = new ModuleSource(
        -100,
        "module_name",
        ModuleSource.Type.INTERPRETER,
        "module_artifact",
        ModuleSource.Status.INSTALLED,
        "module_path",
        true
    );
    sources.add(source);
    return source;
  }

  @Test
  public void persist() {
    final ModuleSource source = getTestModuleSource();
    moduleSourcesDAO.persist(source);
    assertNotEquals(-100, source.getId());
  }

  @Test
  public void get() {
    final ModuleSource source = getTestModuleSource();
    moduleSourcesDAO.persist(source);
    assertEquals(source, moduleSourcesDAO.get(source.getId()));
  }

  @Test
  public void update() {
    final ModuleSource source = getTestModuleSource();
    moduleSourcesDAO.persist(source);

    source.setStatus(ModuleSource.Status.NOT_INSTALLED);
    source.setReinstallOnStart(false);
    moduleSourcesDAO.update(source);

    final ModuleSource dbSource = moduleSourcesDAO.get(source.getId());
    assertEquals(source, dbSource);
  }

  @Test
  public void getAll() {
    final List<ModuleSource> sources = new ArrayList<>(3);
    ModuleSource source = getTestModuleSource();
    source.setName("module_name1");
    source.setArtifact("artifact1");
    source.setPath("module_path1");
    sources.add(source);

    source = getTestModuleSource();
    source.setName("module_name2");
    source.setArtifact("artifact2");
    source.setPath("module_path2");
    sources.add(source);

    source = getTestModuleSource();
    source.setName("module_name3");
    source.setArtifact("artifact3");
    source.setPath("module_path3");
    sources.add(source);

    sources.forEach(moduleSourcesDAO::persist);

    final List<ModuleSource> allSources = moduleSourcesDAO.getAll();
    allSources.sort(Comparator.comparingLong(ModuleSource::getId));
    assertEquals(3, allSources.size());
    assertEquals(sources.get(0), allSources.get(0));
    assertEquals(sources.get(1), allSources.get(1));
    assertEquals(sources.get(2), allSources.get(2));
  }

  @Test
  public void delete() {
    final ModuleSource source = getTestModuleSource();
    moduleSourcesDAO.persist(source);
    moduleSourcesDAO.delete(source.getId());
    assertNull(moduleSourcesDAO.get(source.getId()));
  }
}
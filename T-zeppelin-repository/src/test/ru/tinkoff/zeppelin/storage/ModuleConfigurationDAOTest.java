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
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleSource;
import ru.tinkoff.zeppelin.core.configuration.interpreter.option.Permissions;

import java.util.*;

import static org.junit.Assert.*;

public class ModuleConfigurationDAOTest extends AbstractTest {

  private static ModuleConfigurationDAO moduleConfigurationDAO;

  private static ModuleInnerConfiguration innerConfiguration;
  private static ModuleSource moduleSource;

  @BeforeClass
  public static void init() {
    moduleConfigurationDAO = new ModuleConfigurationDAO(jdbcTemplate);
  }

  @Before
  public void forEach() {
    innerConfiguration = ModuleInnerConfigurationDAOTest.getTestModuleInnerConfiguration();
    new ModuleInnerConfigurationDAO(jdbcTemplate).persist(innerConfiguration);
    moduleSource = ModuleSourcesDAOTest.getTestModuleSource();
    new ModuleSourcesDAO(jdbcTemplate).persist(moduleSource);
  }

  @After
  public void afterEach() {
    new ModuleInnerConfigurationDAO(jdbcTemplate).delete(innerConfiguration.getId());
    new ModuleSourcesDAO(jdbcTemplate).delete(moduleSource.getId());

    moduleConfigurations.forEach(mc -> moduleConfigurationDAO.delete(mc.getId()));
    moduleConfigurations.clear();
  }

  private static final List<ModuleConfiguration> moduleConfigurations = new ArrayList<>();

  private static ModuleConfiguration getTestModuleConfiguration() {
    final ModuleConfiguration configuration = new ModuleConfiguration(
        -100,
        "pythonCompleter",
        "PythonCompleter",
        "python",
        "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=9846",
        10,
        1000,
        1000,
        innerConfiguration.getId(),
        moduleSource.getId(),
        new Permissions(Collections.singletonList(Person.USERNAME_1), true),
        true
    );
    moduleConfigurations.add(configuration);
    return configuration;
  }

  @Test
  public void persist() {
    final ModuleConfiguration configuration = getTestModuleConfiguration();
    moduleConfigurationDAO.persist(configuration);

    assertNotEquals(-100, configuration.getId());
  }

  @Test
  public void getById() {
    final ModuleConfiguration configuration = getTestModuleConfiguration();
    moduleConfigurationDAO.persist(configuration);

    final ModuleConfiguration dbConfiguration = moduleConfigurationDAO.getById(configuration.getId());
    assertEquals(configuration, dbConfiguration);
  }

  @Test
  public void update() {
    final ModuleConfiguration configuration = getTestModuleConfiguration();
    moduleConfigurationDAO.persist(configuration);

    configuration.setEnabled(false);
    configuration.setBindedTo("Nopython");
    configuration.setConcurrentTasks(100);

    moduleConfigurationDAO.update(configuration);

    final ModuleConfiguration dbConfiguration = moduleConfigurationDAO.getById(configuration.getId());
    assertEquals(configuration, dbConfiguration);
  }

  @Test
  public void delete() {
    final ModuleConfiguration configuration = getTestModuleConfiguration();
    moduleConfigurationDAO.persist(configuration);
    moduleConfigurationDAO.delete(configuration.getId());
    assertNull(moduleConfigurationDAO.getById(configuration.getId()));
  }

  @Test
  public void getByShebang() {
    final ModuleConfiguration configuration = getTestModuleConfiguration();
    configuration.setShebang("python");
    moduleConfigurationDAO.persist(configuration);

    final ModuleConfiguration dbConfiguration = moduleConfigurationDAO.getByShebang("python");
    assertEquals(configuration, dbConfiguration);
  }

  @Test
  public void getAll() {
    final List<ModuleConfiguration> configurations = Arrays.asList(
        getTestModuleConfiguration(),
        getTestModuleConfiguration(),
        getTestModuleConfiguration()
    );

    for (int i = 0; i < configurations.size(); i++) {
      final ModuleConfiguration configuration = configurations.get(i);
      configuration.setShebang(configuration.getShebang() + i);
    }

    configurations.forEach(c -> {
      c.setModuleInnerConfigId(createAndGetModuleInnerConfiguration().getId());
      c.setModuleSourceId(createAndGetModuleSource().getId());
      moduleConfigurationDAO.persist(c);
    });

    final List<ModuleConfiguration> allConfigurations = moduleConfigurationDAO.getAll();
    allConfigurations.sort(Comparator.comparingLong(ModuleConfiguration::getId));
    assertEquals(configurations.size(), allConfigurations.size());
    assertEquals(configurations.get(0), allConfigurations.get(0));
    assertEquals(configurations.get(1), allConfigurations.get(1));
    assertEquals(configurations.get(2), allConfigurations.get(2));
  }

  private static ModuleInnerConfiguration createAndGetModuleInnerConfiguration() {
    final ModuleInnerConfiguration innerConfiguration = ModuleInnerConfigurationDAOTest.getTestModuleInnerConfiguration();
    final int rand = new Random().nextInt(10000000);
    innerConfiguration.setClassName("ClassName" + rand);
    return new ModuleInnerConfigurationDAO(jdbcTemplate).persist(innerConfiguration);
  }
  private static ModuleSource createAndGetModuleSource() {
    final ModuleSource moduleSource = ModuleSourcesDAOTest.getTestModuleSource();
    final int rand = new Random().nextInt(10000000);
    moduleSource.setName("module_name" + rand);
    moduleSource.setArtifact("artifact" + rand);
    moduleSource.setPath("module_path" + rand);
    return new ModuleSourcesDAO(jdbcTemplate).persist(moduleSource);
  }
}
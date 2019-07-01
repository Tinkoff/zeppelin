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
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleProperty;

import java.util.*;

import static org.junit.Assert.*;

public class ModuleInnerConfigurationDAOTest extends AbstractTest {

  private static ModuleInnerConfigurationDAO moduleInnerConfigurationDAO;
  private final static List<ModuleInnerConfiguration> configurations = new ArrayList<>();

  @BeforeClass
  public static void init() {
    moduleInnerConfigurationDAO = new ModuleInnerConfigurationDAO(jdbcTemplate);
  }

  @After
  public void afterEach() {
    configurations.forEach(c -> moduleInnerConfigurationDAO.delete(c.getId()));
    configurations.clear();
  }

  static ModuleInnerConfiguration getTestModuleInnerConfiguration() {
    final Map<String, ModuleProperty> properties = new HashMap<>();
    properties.put("python.autoimport", new ModuleProperty(
        "PYTHON_AUTOIMPORT",
        "python.autoimport",
        "defaultValue",
        "currentValue",
        "autoimported modules. Separated by ';'. Example module:name;module2:name2 -> import module as name & import module2 as name2",
        "textarea"
    ));

    final Map<String, Object> editor = new HashMap<>();
    editor.put("language", "python");

    final ModuleInnerConfiguration configuration = new ModuleInnerConfiguration(
        -100,
        "ru.tinkoff.zeppelin.interpreter.python.PythonInterpreter",
        properties,
        editor);
    configurations.add(configuration);
    return configuration;
  }

  @Test
  public void persist() {
    final ModuleInnerConfiguration configuration = getTestModuleInnerConfiguration();
    moduleInnerConfigurationDAO.persist(configuration);

    final ModuleInnerConfiguration dbConfiguration = moduleInnerConfigurationDAO.getById(configuration.getId());
    assertNotEquals(-100, dbConfiguration.getId());
  }

  @Test
  public void update() {
    ModuleInnerConfiguration configuration = getTestModuleInnerConfiguration();
    moduleInnerConfigurationDAO.persist(configuration);

    final Map<String, ModuleProperty> properties = configuration.getProperties();
    properties.put("new_pop", new ModuleProperty(
        "envName",
        "propertyName",
        "defaultValue",
        "description",
        "type"
    ));
    final Map<String, Object> editor = configuration.getEditor();
    editor.put("editor_param", "editor_param_value");
    configuration = new ModuleInnerConfiguration(
        configuration.getId(),
        configuration.getClassName(),
        properties,
        editor
    );
    moduleInnerConfigurationDAO.update(configuration);

    final ModuleInnerConfiguration dbConfiguration = moduleInnerConfigurationDAO.getById(configuration.getId());
    assertEquals(configuration, dbConfiguration);
  }

  @Test
  public void getById() {
    final ModuleInnerConfiguration configuration = getTestModuleInnerConfiguration();
    moduleInnerConfigurationDAO.persist(configuration);

    final ModuleInnerConfiguration dbConfiguration = moduleInnerConfigurationDAO.getById(configuration.getId());
    assertEquals(configuration, dbConfiguration);
  }

  @Test
  public void getAll() {
    final List<ModuleInnerConfiguration> configuration = new ArrayList<>();
    ModuleInnerConfiguration testConfig = getTestModuleInnerConfiguration();
    testConfig.setClassName("ClassName1");
    configuration.add(testConfig);
    testConfig = getTestModuleInnerConfiguration();
    testConfig.setClassName("ClassName2");
    configuration.add(testConfig);
    testConfig = getTestModuleInnerConfiguration();
    testConfig.setClassName("ClassName3");
    configuration.add(testConfig);

    configuration.forEach(moduleInnerConfigurationDAO::persist);

    final List<ModuleInnerConfiguration> allConfigurations = moduleInnerConfigurationDAO.getAll();
    allConfigurations.sort(Comparator.comparingLong(ModuleInnerConfiguration::getId));
    assertEquals(3, allConfigurations.size());
    assertEquals(configuration.get(0), allConfigurations.get(0));
    assertEquals(configuration.get(1), allConfigurations.get(1));
    assertEquals(configuration.get(2), allConfigurations.get(2));
  }

  @Test
  public void delete() {
    final ModuleInnerConfiguration configuration = getTestModuleInnerConfiguration();
    moduleInnerConfigurationDAO.persist(configuration);

    final ModuleInnerConfiguration dbConfiguration = moduleInnerConfigurationDAO.getById(configuration.getId());
    assertNotEquals(-100, dbConfiguration.getId());

    moduleInnerConfigurationDAO.delete(configuration.getId());
    assertNull(moduleInnerConfigurationDAO.getById(configuration.getId()));
  }
}
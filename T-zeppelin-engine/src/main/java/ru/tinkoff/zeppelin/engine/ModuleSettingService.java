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

package ru.tinkoff.zeppelin.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.Repository;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleProperty;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleSource;
import ru.tinkoff.zeppelin.engine.server.AbstractRemoteProcess;
import ru.tinkoff.zeppelin.engine.server.ModuleInstaller;
import ru.tinkoff.zeppelin.engine.server.RemoteProcessType;
import ru.tinkoff.zeppelin.storage.ModuleConfigurationDAO;
import ru.tinkoff.zeppelin.storage.ModuleInnerConfigurationDAO;
import ru.tinkoff.zeppelin.storage.ModuleRepositoryDAO;
import ru.tinkoff.zeppelin.storage.ModuleSourcesDAO;

@Component
public class ModuleSettingService {

  private final ModuleRepositoryDAO moduleRepositoryDAO;
  private final ModuleSourcesDAO moduleSourcesDAO;
  private final ModuleConfigurationDAO moduleConfigurationDAO;
  private final ModuleInnerConfigurationDAO moduleInnerConfigurationDAO;

  public ModuleSettingService(final ModuleRepositoryDAO moduleRepositoryDAO,
                              final ModuleSourcesDAO moduleSourcesDAO,
                              final ModuleConfigurationDAO moduleConfigurationDAO,
                              final ModuleInnerConfigurationDAO moduleInnerConfigurationDAO) {
    this.moduleRepositoryDAO = moduleRepositoryDAO;
    this.moduleSourcesDAO = moduleSourcesDAO;
    this.moduleConfigurationDAO = moduleConfigurationDAO;
    this.moduleInnerConfigurationDAO = moduleInnerConfigurationDAO;
  }

  @PostConstruct
  private void init() {
    final Collection<ModuleConfiguration> configurations = moduleConfigurationDAO.getAll();
    moduleSourcesDAO.getAll().stream()
            .filter(ms -> ms.isReinstallOnStart() && ms.getStatus() == ModuleSource.Status.INSTALLED
                    || ms.getStatus() == ModuleSource.Status.INSTALLED && !ModuleInstaller.isInstalled(ms.getName()))
            .forEach(src -> {
              uninstallSource(src, false);
              installSource(src, true, false);

              // enable configurations
              for (final ModuleConfiguration configuration : configurations) {
                if (configuration.getModuleSourceId() == src.getId()) {
                  final ModuleConfiguration mc = moduleConfigurationDAO.getById(configuration.getId());
                  mc.setEnabled(true);
                  moduleConfigurationDAO.update(mc);
                }
              }
            });
  }

  public synchronized void installSource(@Nonnull final ModuleSource source,
                                         final boolean installSources,
                                         final boolean checkNames) {

    final Collection<ModuleSource> sources = moduleSourcesDAO.getAll();
    // 1 check that name does not exist
    final boolean hasSameName = sources.stream()
            .anyMatch(s -> s.getName().equals(source.getName()));

    if (hasSameName && checkNames) {
      throw new RuntimeException("Wrong configuration. Found duplicated name.");
    }

    //1 check for ability to install
    final String installationDir;
    try {
      ModuleInstaller.uninstallInterpreter(source.getName());

      final List<String> repos = Configuration.getRepos();
      getAllRepositories().forEach(r -> repos.add(r.getUrl()));

      installationDir = ModuleInstaller.install(source.getName(), source.getArtifact(), repos);
      if (StringUtils.isEmpty(installationDir)) {
        throw new RuntimeException("Empty installation dir path. Can't install module.");
      }

    } catch (final Exception e) {
      ModuleInstaller.uninstallInterpreter(source.getName());
      throw new RuntimeException("Error while install sources: " + e.getMessage());
    }

    //2 check configuration file
    final ModuleInnerConfiguration baseConfig;
    try {
      baseConfig = ModuleInstaller.getDefaultConfig(source.getName());
    } catch (final Exception e) {
      ModuleInstaller.uninstallInterpreter(source.getName());
      throw new RuntimeException("Wrong zeppelin module configuration: " + e.getMessage());
    }

    // update modules configuration
    moduleConfigurationDAO.getAll().stream()
            .filter(mc -> mc.getModuleSourceId() == source.getId())
            .map(mc -> moduleInnerConfigurationDAO.getById(mc.getModuleInnerConfigId()))
            .map(mic -> {
              final Map<String, ModuleProperty> baseProps = baseConfig.getProperties();
              final Map<String, ModuleProperty> currProps = mic.getProperties();

              final List<String> currPropsList = new ArrayList<>(currProps.keySet());
              for (final String name : currPropsList) {
                if (!baseProps.containsKey(name)) {
                  currProps.remove(name);
                }
              }
              final List<String> basePropsList = new ArrayList<>(baseProps.keySet());
              for (final String name : basePropsList) {
                if (!currProps.containsKey(name)) {
                  currProps.put(name, baseProps.get(name));
                }
              }

              final Map<String, Object> baseEditor = baseConfig.getEditor();
              final Map<String, Object> currEditor = mic.getEditor();

              final List<String> currEditorList = new ArrayList<>(currEditor.keySet());
              for (final String name : currEditorList) {
                if (!baseEditor.containsKey(name)) {
                  currEditor.remove(name);
                }
              }

              final List<String> baseEditorList = new ArrayList<>(baseEditor.keySet());
              for (final String name : baseEditorList) {
                if (!currEditor.containsKey(name)) {
                  currEditor.put(name, baseEditor.get(name));
                }
              }
              return mic;
            })
            .forEach(mic -> moduleInnerConfigurationDAO.update(mic));

    if (!installSources) {
      ModuleInstaller.uninstallInterpreter(source.getName());
    }

    if (!StringUtils.isAllBlank(installationDir) && installSources) {
      source.setPath(installationDir);
      source.setStatus(ModuleSource.Status.INSTALLED);
    } else {
      source.setPath(null);
      source.setStatus(ModuleSource.Status.NOT_INSTALLED);
    }
    if (hasSameName) {
      moduleSourcesDAO.update(source);
    } else {
      moduleSourcesDAO.persist(source);
    }
  }


  public synchronized void uninstallSource(@Nonnull final ModuleSource source, final boolean delete) {

    final Collection<ModuleConfiguration> configurations = moduleConfigurationDAO.getAll()
            .stream()
            .filter(c -> c.getModuleSourceId() == source.getId())
            .collect(Collectors.toList());

    // 1 disable all interpreters
    for (final ModuleConfiguration option : configurations) {
      option.setEnabled(false);
      moduleConfigurationDAO.update(option);
    }

    // 2 stop all interpreters
    for (final ModuleConfiguration option : configurations) {
      final String shebang = option.getShebang();
      final AbstractRemoteProcess process = AbstractRemoteProcess.get(shebang, RemoteProcessType.INTERPRETER);
      if (process != null) {
        process.forceKill();
      }
    }

    // 3 uninstall interpreter
    ModuleInstaller.uninstallInterpreter(source.getName());

    // 4 set correct status
    source.setStatus(ModuleSource.Status.NOT_INSTALLED);
    moduleSourcesDAO.update(source);

    // 4 early exit if not delete drom db
    if (!delete) {
      return;
    }

    // 5 delete all configurations
    for (final ModuleConfiguration option : configurations) {
      moduleConfigurationDAO.delete(option.getId());
    }

    // 5 delete sources
    moduleSourcesDAO.delete(source.getId());
  }

  public void restart(final String shebang) {
    for (final RemoteProcessType processType : RemoteProcessType.values()) {
      final AbstractRemoteProcess process = AbstractRemoteProcess.get(shebang, processType);
      if (process != null) {
        process.forceKill();
      }
    }
  }

  @Nonnull
  private List<Repository> getAllRepositories() {
    return moduleRepositoryDAO.getAll();
  }

}

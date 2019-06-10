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
package org.apache.zeppelin.rest;

import com.google.gson.Gson;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zeppelin.Repository;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleSource;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleSource.Type;
import ru.tinkoff.zeppelin.core.configuration.interpreter.option.Permissions;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.engine.ModuleSettingService;
import ru.tinkoff.zeppelin.engine.server.ModuleInstaller;
import ru.tinkoff.zeppelin.storage.ModuleConfigurationDAO;
import ru.tinkoff.zeppelin.storage.ModuleInnerConfigurationDAO;
import ru.tinkoff.zeppelin.storage.ModuleRepositoryDAO;
import ru.tinkoff.zeppelin.storage.ModuleSourcesDAO;

@RestController
@RequestMapping("/api/modules")
public class ModuleRestApi {

  private final ModuleSourcesDAO moduleSourcesDAO;
  private final ModuleRepositoryDAO moduleRepositoryDAO;
  private final ModuleConfigurationDAO moduleConfigurationDAO;
  private final ModuleInnerConfigurationDAO moduleInnerConfigurationDAO;
  private final ModuleSettingService moduleSettingService;

  public ModuleRestApi(final ModuleSourcesDAO moduleSourcesDAO,
                       final ModuleConfigurationDAO moduleConfigurationDAO,
                       final ModuleInnerConfigurationDAO moduleInnerConfigurationDAO,
                       final ModuleSettingService moduleSettingService,
                       final ModuleRepositoryDAO moduleRepositoryDAO) {

    this.moduleSourcesDAO = moduleSourcesDAO;
    this.moduleRepositoryDAO = moduleRepositoryDAO;
    this.moduleConfigurationDAO = moduleConfigurationDAO;
    this.moduleInnerConfigurationDAO = moduleInnerConfigurationDAO;
    this.moduleSettingService = moduleSettingService;
  }

  public static class ModuleSettingsDTO {

    public static class ModuleDTO {
      public ModuleConfiguration configuration;
      public ModuleInnerConfiguration innerConfiguration;
    }

    public static class ModuleSettingBlockDTO {
      public ModuleSource moduleSource;
      public ModuleInnerConfiguration defaultModuleConfiguration;
      public List<ModuleDTO> modules = new ArrayList<>();
    }

    public List<ModuleSettingBlockDTO> modules = new ArrayList<>();
  }

  @GetMapping(value = "/list", produces = "application/json")
  public ResponseEntity listModules() {
    try {
      if(!isAdmin()) {
        return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
      }

      ModuleSettingsDTO moduleSettingsDTO = new ModuleSettingsDTO();

      final List<ModuleSource> moduleSources = moduleSourcesDAO.getAll();
      final List<ModuleConfiguration> moduleConfigurations = moduleConfigurationDAO.getAll();

      for (final ModuleSource moduleSource : moduleSources) {
        ModuleSettingsDTO.ModuleSettingBlockDTO moduleSettingBlockDTO = new ModuleSettingsDTO.ModuleSettingBlockDTO();
        moduleSettingBlockDTO.moduleSource = moduleSource;

        // load defaultConfiguration
        if (moduleSource.getStatus() == ModuleSource.Status.INSTALLED) {
          moduleSettingBlockDTO.defaultModuleConfiguration
                  = ModuleInstaller.getDefaultConfig(moduleSource.getName());
        }

        final List<ModuleConfiguration> configurations = moduleConfigurations.stream()
                .filter(mc -> mc.getModuleSourceId() == moduleSource.getId())
                .collect(Collectors.toList());

        for (final ModuleConfiguration moduleConfiguration : configurations) {
          final ModuleInnerConfiguration moduleInnerConfiguration = moduleInnerConfigurationDAO.getById(moduleConfiguration.getModuleInnerConfigId());

          final ModuleSettingsDTO.ModuleDTO moduleDTO = new ModuleSettingsDTO.ModuleDTO();
          moduleDTO.configuration = moduleConfiguration;
          moduleDTO.innerConfiguration = moduleInnerConfiguration;
          moduleSettingBlockDTO.modules.add(moduleDTO);
        }

        moduleSettingBlockDTO.modules.sort(Comparator.comparing(o -> o.configuration.getShebang()));
        moduleSettingsDTO.modules.add(moduleSettingBlockDTO);
      }

      moduleSettingsDTO.modules.sort(Comparator.comparing(o -> o.moduleSource.getName()));

      return new JsonResponse(HttpStatus.OK, "", moduleSettingsDTO).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  public static class InstallModuleConfigurationDTO {
    public long id;
  }

  @PostMapping(value = "/installModuleSource", produces = "application/json")
  public ResponseEntity installModuleConfiguration(@RequestBody final String message) {
    try {
      if(!isAdmin()) {
        return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
      }

      final InstallModuleConfigurationDTO installModuleConfigurationDTO = new Gson().fromJson(message, InstallModuleConfigurationDTO.class);
      final ModuleSource moduleSource = moduleSourcesDAO.get(installModuleConfigurationDTO.id);

      moduleSettingService.installSource(moduleSource, true, false);

      return new JsonResponse(HttpStatus.OK, "", "").build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  public static class UninstallModuleConfigurationDTO {
    public long id;
  }

  @PostMapping(value = "/uninstallModuleSource", produces = "application/json")
  public ResponseEntity uninstallModuleSource(@RequestBody final String message) {
    try {
      if(!isAdmin()) {
        return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
      }

      final UninstallModuleConfigurationDTO uninstallModuleConfigurationDTO = new Gson().fromJson(message, UninstallModuleConfigurationDTO.class);
      final ModuleSource moduleSource = moduleSourcesDAO.get(uninstallModuleConfigurationDTO.id);

      moduleSettingService.uninstallSource(moduleSource, false);

      return new JsonResponse(HttpStatus.OK, "", "").build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  public static class SetReinstallOnStartModuleSourceDTO {
    public long id;
    public boolean reinstall;
  }

  @PostMapping(value = "/setReinstallOnStartModuleSource", produces = "application/json")
  public ResponseEntity setReinstallOnStartModuleSource(@RequestBody final String message) {
    try {
      if(!isAdmin()) {
        return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
      }

      final SetReinstallOnStartModuleSourceDTO setReinstallOnStartModuleSourceDTO = new Gson().fromJson(message, SetReinstallOnStartModuleSourceDTO.class);
      final ModuleSource moduleSource = moduleSourcesDAO.get(setReinstallOnStartModuleSourceDTO.id);

      moduleSource.setReinstallOnStart(setReinstallOnStartModuleSourceDTO.reinstall);
      moduleSourcesDAO.update(moduleSource);

      return new JsonResponse(HttpStatus.OK, "", "").build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  public static class AddModuleSourceDTO {
    public String name;
    public String artifact;
    public String type;
  }

  @PostMapping(value = "/addModuleSource", produces = "application/json")
  public ResponseEntity addModuleSource(@RequestBody final String message) {
    try {
      if(!isAdmin()) {
        return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
      }

      final AddModuleSourceDTO addModuleSourceDTO = new Gson().fromJson(message, AddModuleSourceDTO.class);

      final ModuleSource moduleSource = new ModuleSource(
              -1,
              addModuleSourceDTO.name,
              ModuleSource.Type.valueOf(addModuleSourceDTO.type),
              addModuleSourceDTO.artifact,
              ModuleSource.Status.NOT_INSTALLED,
              StringUtils.EMPTY,
              false
      );

      moduleSettingService.installSource(moduleSource, false, true);
      return new JsonResponse(HttpStatus.OK, "", "").build();

    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  public static class DeleteModuleSourceDTO {
    public long id;
  }

  @PostMapping(value = "/deleteModuleSource", produces = "application/json")
  public ResponseEntity deleteModuleSource(@RequestBody final String message) {
    try {
      if(!isAdmin()) {
        return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
      }

      final DeleteModuleSourceDTO deleteModuleSourceDTO = new Gson().fromJson(message, DeleteModuleSourceDTO.class);

      final List<ModuleConfiguration> configurations = moduleConfigurationDAO.getAll().stream()
              .filter(c -> c.getModuleSourceId() == deleteModuleSourceDTO.id)
              .collect(Collectors.toList());

      for (final ModuleConfiguration configuration : configurations) {
        configuration.setEnabled(false);
        moduleConfigurationDAO.update(configuration);
        moduleSettingService.restart(configuration.getShebang());
        moduleConfigurationDAO.delete(configuration.getId());
      }

      moduleSourcesDAO.delete(deleteModuleSourceDTO.id);

      return new JsonResponse(HttpStatus.OK, "", "").build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  public static class AddModuleConfigurationDTO {
    public ModuleConfiguration moduleConfiguration;
    public ModuleInnerConfiguration innerConfiguration;
  }
  @PostMapping(value = "/addModuleConfiguration", produces = "application/json")
  public ResponseEntity addModuleConfiguration(@RequestBody final String message) {
    try {
      if(!isAdmin()) {
        return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
      }

      final AddModuleConfigurationDTO addModuleConfigurationDTO = new Gson().fromJson(message, AddModuleConfigurationDTO.class);

      if (!addModuleConfigurationDTO.moduleConfiguration.getShebang().matches("\\w+")) {
        return new JsonResponse(
            HttpStatus.BAD_REQUEST,
            "Shebang is incorrect, it must contain only alphanumeric characters and \"_\""
        ).build();
      }

      if (addModuleConfigurationDTO.moduleConfiguration.getHumanReadableName().trim().isEmpty()
          || addModuleConfigurationDTO.moduleConfiguration.getShebang().trim().isEmpty()) {
        return new JsonResponse(HttpStatus.BAD_REQUEST, "Please fill in interpreter name and shebang").build();
      }

      final ModuleInnerConfiguration innerConfiguration
              = moduleInnerConfigurationDAO.persist(addModuleConfigurationDTO.innerConfiguration);

      final ModuleConfiguration moduleConfiguration = addModuleConfigurationDTO.moduleConfiguration;
      moduleConfiguration.setModuleInnerConfigId(innerConfiguration.getId());
      moduleConfigurationDAO.persist(moduleConfiguration);

      return new JsonResponse(HttpStatus.OK, "", "").build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  public static class UpdateModuleConfigurationDTO {
    public ModuleConfiguration moduleConfiguration;
    public ModuleInnerConfiguration innerConfiguration;
  }
  @PostMapping(value = "/updateModuleConfiguration", produces = "application/json")
  public ResponseEntity updateModuleConfiguration(@RequestBody final String message) {

    if(!isAdmin()) {
      return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
    }

    final UpdateModuleConfigurationDTO updateModuleConfigurationDTO = new Gson().fromJson(message, UpdateModuleConfigurationDTO.class);
    moduleInnerConfigurationDAO.update(updateModuleConfigurationDTO.innerConfiguration);
    moduleConfigurationDAO.update(updateModuleConfigurationDTO.moduleConfiguration);

    moduleSettingService.restart(updateModuleConfigurationDTO.moduleConfiguration.getShebang());

    return new JsonResponse(HttpStatus.OK, "", "").build();
  }


  public static class EnableModuleDTO {
    public long id;
    public boolean enable;
  }
  @PostMapping(value = "/enableModule", produces = "application/json")
  public ResponseEntity enableModule(@RequestBody final String message) {
    try {
      if(!isAdmin()) {
        return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
      }

      final EnableModuleDTO enableModuleDTO = new Gson().fromJson(message, EnableModuleDTO.class);
      final ModuleConfiguration moduleConfiguration = moduleConfigurationDAO.getById(enableModuleDTO.id);
      moduleConfiguration.setEnabled(enableModuleDTO.enable);
      moduleConfigurationDAO.update(moduleConfiguration);

      moduleSettingService.restart(moduleConfiguration.getShebang());

      return new JsonResponse(HttpStatus.OK, "", "").build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  public static class RestartModuleDTO {
    public long id;
  }
  @PostMapping(value = "/restartModule", produces = "application/json")
  public ResponseEntity restartModule(@RequestBody final String message) {
    try {
      if(!isAdmin()) {
        return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
      }

      final RestartModuleDTO restartModuleDTO = new Gson().fromJson(message, RestartModuleDTO.class);
      final ModuleConfiguration moduleConfiguration = moduleConfigurationDAO.getById(restartModuleDTO.id);
      moduleSettingService.restart(moduleConfiguration.getShebang());

      return new JsonResponse(HttpStatus.OK, "", "").build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  public static class DeleteModuleDTO {
    public long id;
  }
  @PostMapping(value = "/deleteModule", produces = "application/json")
  public ResponseEntity deleteModule(@RequestBody final String message) {
    try {
      if(!isAdmin()) {
        return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
      }

      final DeleteModuleDTO deleteModuleDTO = new Gson().fromJson(message, DeleteModuleDTO.class);
      final ModuleConfiguration moduleConfiguration = moduleConfigurationDAO.getById(deleteModuleDTO.id);
      moduleConfigurationDAO.delete(moduleConfiguration.getId());
      moduleInnerConfigurationDAO.delete(moduleConfiguration.getModuleInnerConfigId());

      moduleSettingService.restart(moduleConfiguration.getShebang());

      return new JsonResponse(HttpStatus.OK, "", "").build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }


  /**
   * List all interpreter settings.
   */
  public static class ConfigurationDTO {
    public long id;
    public String shebang;
    public String humanReadableName;
    public String bindedTo;
    public String jvmOptions;
    public int concurrentTasks;
    public long moduleInnerConfigId;
    public long moduleSourceId;
    public Permissions permissions;
    public boolean isEnabled;

    public ModuleInnerConfiguration config;
  }

  @GetMapping(value = "/setting", produces = "application/json")
  public ResponseEntity listSettings() {
    try {
      if(!isAdmin()) {
        return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
      }

      final List<ConfigurationDTO> result = new ArrayList<>();
      final List<ModuleConfiguration> configurations = moduleConfigurationDAO.getAll();
      for (final ModuleConfiguration configuration : configurations) {
        final ModuleInnerConfiguration inner = moduleInnerConfigurationDAO.getById(configuration.getModuleInnerConfigId());
        final ConfigurationDTO conf = new ConfigurationDTO();
        conf.id = configuration.getId();
        conf.shebang = configuration.getShebang();
        conf.humanReadableName = configuration.getHumanReadableName();
        conf.bindedTo = configuration.getBindedTo();
        conf.jvmOptions = configuration.getJvmOptions();
        conf.concurrentTasks = configuration.getConcurrentTasks();
        conf.permissions = configuration.getPermissions();
        conf.isEnabled = configuration.isEnabled();
        conf.config = inner;
        result.add(conf);
      }

      return new JsonResponse(HttpStatus.OK, "", result).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * Lists only interpreters.
   *
   * @return
   */
  @GetMapping(value = "/setting/interpreters", produces = "application/json")
  public ResponseEntity listInterpretersSettings() {
    try {

      if(!isAdmin()) {
        return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
      }

      final List<ConfigurationDTO> result = new ArrayList<>();
      final List<ModuleConfiguration> configurations = moduleConfigurationDAO.getAll();
      for (final ModuleConfiguration configuration : configurations) {
        final ModuleSource source = moduleSourcesDAO.get(configuration.getModuleSourceId());
        if (source != null && source.getType().equals(Type.COMPLETER)) {
          continue;
        }

        final ModuleInnerConfiguration inner = moduleInnerConfigurationDAO.getById(configuration.getModuleInnerConfigId());
        final ConfigurationDTO conf = new ConfigurationDTO();
        conf.id = configuration.getId();
        conf.shebang = configuration.getShebang();
        conf.humanReadableName = configuration.getHumanReadableName();
        conf.bindedTo = configuration.getBindedTo();
        conf.jvmOptions = configuration.getJvmOptions();
        conf.concurrentTasks = configuration.getConcurrentTasks();
        conf.permissions = configuration.getPermissions();
        conf.isEnabled = configuration.isEnabled();
        conf.config = inner;
        result.add(conf);
      }

      return new JsonResponse(HttpStatus.OK, "", result).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * List of dependency resolving repositories.
   */
  @GetMapping(value = "/repository", produces = "application/json")
  public ResponseEntity listRepositories() {
    try {
      if(!isAdmin()) {
        return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
      }

      return new JsonResponse(HttpStatus.OK, "", moduleRepositoryDAO.getAll()).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * Add new repository.
   *
   * @param message Repository
   */
  @PostMapping(value = "/repository", produces = "application/json")
  public ResponseEntity addRepository(@RequestBody final String message) {
    try {
      if(!isAdmin()) {
        return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
      }

      final Repository request = Repository.fromJson(message);
      if (!request.getId().matches("\\w+")) {
        return new JsonResponse(
            HttpStatus.BAD_REQUEST,
            "Id is incorrect, it must contain only alphanumeric characters and \"_\""
        ).build();
      }
      moduleRepositoryDAO.persist(request);
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * Delete repository.
   *
   * @param repoId ID of repository
   */
  @DeleteMapping(value = "/repository/{repoId}", produces = "application/json")
  public ResponseEntity removeRepository(@PathVariable("repoId") final String repoId) {
    try {
      if(!isAdmin()) {
        return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
      }

      moduleRepositoryDAO.delete(repoId);
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
  }

  private boolean isAdmin() {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Set<String> userRoles = new HashSet<>();
    userRoles.add(authenticationInfo.getUser());
    userRoles.addAll(authenticationInfo.getRoles());

    final Set<String> admin = new HashSet<>();
    admin.addAll(Configuration.getAdminUsers());
    admin.addAll(Configuration.getAdminGroups());
    for (final String availableRole : userRoles) {
      if (admin.contains(availableRole)) {
        return true;
      }
    }
    return false;
  }

}

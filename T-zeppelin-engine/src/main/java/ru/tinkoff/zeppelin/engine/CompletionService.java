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

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleSource;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.forms.FormsProcessor;
import ru.tinkoff.zeppelin.engine.server.AbstractRemoteProcess;
import ru.tinkoff.zeppelin.engine.server.CompleterRemoteProcess;
import ru.tinkoff.zeppelin.engine.server.RemoteProcessStarter;
import ru.tinkoff.zeppelin.engine.server.RemoteProcessType;
import ru.tinkoff.zeppelin.interpreter.InterpreterCompletion;
import ru.tinkoff.zeppelin.interpreter.NoteContext;
import ru.tinkoff.zeppelin.interpreter.UserContext;
import ru.tinkoff.zeppelin.storage.ModuleConfigurationDAO;
import ru.tinkoff.zeppelin.storage.ModuleInnerConfigurationDAO;
import ru.tinkoff.zeppelin.storage.ModuleSourcesDAO;

import java.util.*;

@DependsOn({"configuration", "thriftBootstrap"})
@Component
public class CompletionService {

  private final ModuleConfigurationDAO moduleConfigurationDAO;
  private final ModuleInnerConfigurationDAO moduleInnerConfigurationDAO;
  private final ModuleSourcesDAO moduleSourcesDAO;
  private final ThriftServerBootstrap serverBootstrap;
  private final CredentialService credentialService;

  public CompletionService(final ModuleConfigurationDAO moduleConfigurationDAO,
                           final ModuleInnerConfigurationDAO moduleInnerConfigurationDAO,
                           final ModuleSourcesDAO moduleSourcesDAO,
                           final ThriftServerBootstrap serverBootstrap,
                           final CredentialService credentialService) {

    this.moduleConfigurationDAO = moduleConfigurationDAO;
    this.moduleInnerConfigurationDAO = moduleInnerConfigurationDAO;
    this.moduleSourcesDAO = moduleSourcesDAO;
    this.serverBootstrap = serverBootstrap;
    this.credentialService = credentialService;
  }

  public List<InterpreterCompletion> complete(final Note note,
                                              final Paragraph paragraph,
                                              final String payload,
                                              final int cursorPosition,
                                              final String user,
                                              final Set<String> roles) {
    try {
      final Collection<ModuleConfiguration> configurations = moduleConfigurationDAO.getAll();

      final ModuleConfiguration config = configurations.stream()
              .filter(configuration -> paragraph.getShebang().equals(configuration.getBindedTo()))
              .filter(c -> moduleSourcesDAO.get(c.getModuleSourceId()).getType() == ModuleSource.Type.COMPLETER)
              .findFirst()
              .orElse(null);

      if (config == null) {
        return new ArrayList<>();
      }
      final ModuleInnerConfiguration innerConfig = moduleInnerConfigurationDAO.getById(config.getModuleInnerConfigId());

      final AbstractRemoteProcess process = AbstractRemoteProcess.get(
              config.getShebang(),
              RemoteProcessType.COMPLETER
      );
      if (process != null && process.getStatus() == AbstractRemoteProcess.Status.READY) {

        final FormsProcessor.InjectResponse response
                = FormsProcessor.injectFormValues(payload, cursorPosition, paragraph.getFormParams());

        // prepare notecontext
        final Map<String, String> noteContext = new HashMap<>();

        noteContext.put(NoteContext.Z_ENV_NOTE_ID.name(), String.valueOf(note.getId()));
        noteContext.put(NoteContext.Z_ENV_NOTE_UUID.name(), String.valueOf(note.getUuid()));
        noteContext.put(NoteContext.Z_ENV_PARAGRAPH_ID.name(), String.valueOf(paragraph.getId()));
        noteContext.put(NoteContext.Z_ENV_PARAGRAPH_SHEBANG.name(), paragraph.getShebang());

        // prepare usercontext
        final Map<String, String> userContext = new HashMap<>();
        userContext.put(UserContext.Z_ENV_USER_NAME.name(), user);
        userContext.put(UserContext.Z_ENV_USER_ROLES.name(), roles.toString());

        // put all available credentials
        credentialService.getUserReadableCredentials(user, roles, false)
                .forEach(c -> userContext.put(c.getKey(), StringUtils.EMPTY));

        // prepare configuration
        final Map<String, String> configuration = new HashMap<>();
        innerConfig.getProperties()
                .forEach((p, v) -> configuration.put(p, String.valueOf(v.getCurrentValue())));

        final String result = ((CompleterRemoteProcess) process).complete(
                response.getPayload(),
                response.getCursorPosition(),
                noteContext,
                userContext,
                configuration);

        return Lists.newArrayList(new Gson().fromJson(result, InterpreterCompletion[].class));

      } else if (process != null && process.getStatus() == AbstractRemoteProcess.Status.STARTING) {
        return new ArrayList<>();

      } else {

        final ModuleSource source = moduleSourcesDAO.get(config.getModuleSourceId());

        if (!config.isEnabled() || source == null || source.getStatus() != ModuleSource.Status.INSTALLED) {
          return new ArrayList<>();
        }

        RemoteProcessStarter.start(
                config.getShebang(),
                RemoteProcessType.COMPLETER,
                source.getPath(),
                innerConfig.getClassName(),
                serverBootstrap.getServer().getAddr(),
                serverBootstrap.getServer().getPort(),
                config.getJvmOptions(),
                config.getConcurrentTasks(),
                config.getRegularTTL(),
                config.getScheduledTTL(),
                Configuration.getInstanceMarkerPrefix());
      }
    } catch (final Exception e) {
      //log this
    }
    return new ArrayList<>();
  }
}

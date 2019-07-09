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
package ru.tinkoff.zeppelin.engine.server;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.zeppelin.DependencyResolver;
import org.apache.zeppelin.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.zeppelin.SystemEvent;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration;
import ru.tinkoff.zeppelin.engine.BuildInfoProvider;
import ru.tinkoff.zeppelin.storage.SystemEventType.ET;
import ru.tinkoff.zeppelin.storage.ZLog;


/**
 * Class for installing interpreters
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
public class ModuleInstaller {

  private static final Logger LOG = LoggerFactory.getLogger(ModuleInstaller.class);
  private static final String DESTINATION_FOLDER = "interpreters/";

  private ModuleInstaller() {
    // not called.
  }

  public static boolean isInstalled(final String name) {
    final File folderToStore = new File(DESTINATION_FOLDER + name + "/");
    return folderToStore.exists() && Objects.requireNonNull(folderToStore.list()).length > 0;
  }

  public static String install(final String name, final String artifact, final List<String> repositories) {
    ZLog.log(ET.MODULE_INSTALL,
        String.format("Начата установка модуля[name=%s, artifact=%s] репозитории=%s", name, artifact, repositories.toString()),
        SystemEvent.SYSTEM_USERNAME
    );
    if (isInstalled(name)) {
      final String path = getDirectory(name);
      ZLog.log(ET.MODULE_ALREADY_INSTALLED,
          String.format("Модуль[name=%s, artifact=%s], уже установлен в %s", name, artifact, path),
          SystemEvent.SYSTEM_USERNAME);
      return path;
    }

    final File folderToStore = new File(DESTINATION_FOLDER + name + "/");
    try {
      String artifactToLoad = artifact;
      if (artifactToLoad.toLowerCase().endsWith(":default")) {
        artifactToLoad = Pattern.compile(":default$", Pattern.CASE_INSENSITIVE)
            .matcher(artifactToLoad)
            .replaceAll(":" + BuildInfoProvider.getVersion());

        ZLog.log(ET.MODULE_VERSION_SUBMITTED,
            String.format(
                "Для модуля[name=%s] установлена системная версия, модуль будет установлен по артефакту: %s",
                name, artifactToLoad
            ),
            String.format(
                "Так как модуль добавлен с версией \"default\", то "
                + "при его установке версия была заменена на системную %s",
                BuildInfoProvider.getVersion()
            ),
            SystemEvent.SYSTEM_USERNAME);
      }

      DependencyResolver.load(repositories, artifactToLoad, folderToStore);
      ZLog.log(ET.MODULE_SUCCESSFULLY_INSTALLED,
          String.format("Модуль \"%s\" успешно установлен [%s]", name, folderToStore.getAbsolutePath()),
          SystemEvent.SYSTEM_USERNAME
      );
      return folderToStore.getAbsolutePath();
    } catch (final Exception e) {
      LOG.error("Error while install interpreter", e);
      ZLog.log(ET.MODULE_INSTALLATION_FAILED,
          String.format("Ошибка при установке модуля \"%s\"", name),
          String.format("Ошибка при установке модуля[name=%s;artifact=%s,destination folder=%s], ошибка: %s",
              name, artifact, folderToStore.getAbsolutePath(), e.getMessage()));
      uninstallInterpreter(name);
      return "";
    }
  }

  public static void uninstallInterpreter(final String name) {
    final File folderToStore = new File(DESTINATION_FOLDER + name + "/");
    try {
      FileUtils.deleteDirectory(folderToStore);
      ZLog.log(ET.MODULE_SUCCESSFULLY_UNINSTALLED,
          String.format("Модуль \"%s\" успешно удален", name),
          SystemEvent.SYSTEM_USERNAME);
    } catch (final Exception e) {
      LOG.error("Error while remove interpreter", e);
      ZLog.log(ET.MODULE_DELETION_FAILED,
              String.format("Ошибка при удалении модуля \"%s\"", name),
              String.format("Ошибка при удалении модуля[path=%s], ошибка: %s",
                  folderToStore.getAbsolutePath(), e.getMessage()));
    }
  }

  public static ModuleInnerConfiguration getDefaultConfig(final String name) {
    final File folderToStore = new File(DESTINATION_FOLDER + name + "/");
    ZLog.log(ET.MODULE_CONFIGURATION_REQUESTED,
            String.format("Попытка получения конфигурации модуля \"%s\"", name),
            String.format("Попытка прочитать файл \"interpreter-setting.json\" в %s",
                folderToStore.getAbsolutePath()), SystemEvent.SYSTEM_USERNAME);

    URLClassLoader classLoader = null;
    try {
      final List<URL> urls = Lists.newArrayList();
      for (final File file : folderToStore.listFiles()) {
        final URL url = file.toURI().toURL();

        urls.add(new URL("jar:" + url.toString() + "!/"));
      }

      classLoader = URLClassLoader.newInstance(urls.toArray(new URL[0]));
      final String config = IOUtils.toString(classLoader.getResourceAsStream("interpreter-setting.json"), "UTF-8");
      final ModuleInnerConfiguration result = new Gson().fromJson(config, ModuleInnerConfiguration.class);
      ZLog.log(ET.MODULE_CONFIGURAION_FOUND,
              String.format("Конфигурация для модуля \"%s\" успешно получена", name),
              String.format("Файл \"interpreter-setting.json\" для модуля \"%s\" в %s успешно считан",
                      name, folderToStore.getAbsolutePath()), SystemEvent.SYSTEM_USERNAME);
      return result;
    } catch (final Exception e) {
      ZLog.log(ET.MODULE_CONFIGURATION_PROCESSING_FAILED,
              String.format("Ошибка при получении конфигурации для модуля \"%s\"", name),
              String.format("Ошибка при получении конфигурации для модуля[name=%s, path=%s], ошибка: %s",
                      name, folderToStore.getAbsolutePath(), e.getMessage()), SystemEvent.SYSTEM_USERNAME);
      throw new IllegalArgumentException("Wrong config format", e);
    } finally {
      if (classLoader != null) {
        try {
          classLoader.close();
        } catch (final IOException e) {
          LOG.error("Failed to process config", e);
        }
      }
    }
  }

  public static String getDirectory(final String name) {
    final File folderToStore = new File(DESTINATION_FOLDER + name + "/");
    return folderToStore.getAbsolutePath();
  }
}

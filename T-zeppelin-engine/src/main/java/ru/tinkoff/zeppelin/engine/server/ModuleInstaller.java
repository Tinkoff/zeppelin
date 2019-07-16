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
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.zeppelin.DependencyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration;
import ru.tinkoff.zeppelin.engine.BuildInfoProvider;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;


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
    if (isInstalled(name)) {
      final String path = getDirectory(name);
      return path;
    }

    final File folderToStore = new File(DESTINATION_FOLDER + name + "/");
    try {
      String artifactToLoad = artifact;
      if (artifactToLoad.toLowerCase().endsWith(":default")) {
        artifactToLoad = Pattern.compile(":default$", Pattern.CASE_INSENSITIVE)
            .matcher(artifactToLoad)
            .replaceAll(":" + BuildInfoProvider.getVersion());
      }

      DependencyResolver.load(repositories, artifactToLoad, folderToStore);
      return folderToStore.getAbsolutePath();
    } catch (final Throwable e) {
      uninstallInterpreter(name);
      return "";
    }
  }

  public static void uninstallInterpreter(final String name) {
    final File folderToStore = new File(DESTINATION_FOLDER + name + "/");
    try {
      FileUtils.deleteDirectory(folderToStore);
    } catch (final Throwable e) {
      // SKIP
    }
  }

  public static ModuleInnerConfiguration getDefaultConfig(final String name) {
    final File folderToStore = new File(DESTINATION_FOLDER + name + "/");
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
      return result;
    } catch (final Exception e) {
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

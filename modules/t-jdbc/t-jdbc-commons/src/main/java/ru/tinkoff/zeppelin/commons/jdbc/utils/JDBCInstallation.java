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

package ru.tinkoff.zeppelin.commons.jdbc.utils;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.zeppelin.DependencyResolver;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Objects;

public class JDBCInstallation {

  public static boolean isInstalled(@Nonnull final String artifact) {
    final File folderToStore = new File(getDestinationFolder(artifact));
    return folderToStore.exists() && Objects.requireNonNull(folderToStore.list()).length > 0;
  }

  /**
   * Downloads driver by the maven artifact.
   *
   * @param artifact Driver artifact, never {@code null}.
   * @return Absolute path to driver directory, {@code null} if installation failed.
   */
  @Nullable
  public static String installDriver(@Nonnull final String artifact,
                                     final List<String> dependency,
                                     final String repositoryURL) {
    if (isInstalled(artifact)) {
      return getDirectory(artifact);
    }

    final List<String> repos = Lists.newArrayList(repositoryURL);

    try {
      final File folderToStore = new File(getDestinationFolder(artifact));
      DependencyResolver.load(repos, artifact, folderToStore);
      for (final String dep : dependency) {
        DependencyResolver.load(repos, dep, folderToStore);
      }
      return folderToStore.getAbsolutePath();
    } catch (final Exception e) {
      uninstallDriver(artifact);
      return null;
    }
  }

  public static void uninstallDriver(@Nonnull final String artifact) {
    try {
      if (isInstalled(artifact)) {
        final File folderToStore = new File(getDirectory(artifact));
        FileUtils.deleteDirectory(folderToStore);
      }
    } catch (final Exception e) {
      // SKIP
    }
  }

  /**
   * Gets driver folder, notice that this method should be called after
   * {@link JDBCInstallation#isInstalled(String)}.
   *
   * @param artifact driver maven artifact, never {@code null}.
   * @return absolute path to driver folder.
   */
  @Nonnull
  public static String getDirectory(@Nonnull final String artifact) {
    final File folderToStore = new File(getDestinationFolder(artifact));
    return folderToStore.getAbsolutePath();
  }

  public static String getDestinationFolder(final String artifact) {
    try {
      return String.join(File.separator, "drivers", URLEncoder.encode(artifact, "UTF-16")) + File.separator;
    } catch (final UnsupportedEncodingException e) {
      return "";
    }
  }
}

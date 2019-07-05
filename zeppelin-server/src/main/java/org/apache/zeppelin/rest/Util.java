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

import java.util.Properties;
import org.apache.commons.lang.StringUtils;

public class Util {
  private static final String BRANCH_NAME_PROPERTY = "branch.name";
  private static final String COMMMIT_HASH_PROPERTY = "commit.hash";
  private static final String APPLICATION_VERSION_PROPERTY = "application.version";
  private static final String BUILD_TIME_PROPERTY = "build.timestamp";

  private static final Properties projectProperties;

  static {
    projectProperties = new Properties();
    try {
      projectProperties.load(Util.class.getResourceAsStream("/build.properties"));
    } catch (final Exception e) {
      //Fail to read project.properties
    }
  }

  /**
   * Get Zeppelin version
   *
   * @return Current Zeppelin version
   */
  public static String getVersion() {
    return StringUtils.defaultIfEmpty(projectProperties.getProperty(APPLICATION_VERSION_PROPERTY),
            StringUtils.EMPTY);
  }

  /**
   * Get Zeppelin Git latest commit id
   *
   * @return Latest Zeppelin commit id
   */
  static String getGitCommitId() {
    return StringUtils.defaultIfEmpty(projectProperties.getProperty(COMMMIT_HASH_PROPERTY),
            StringUtils.EMPTY);
  }

  /**
   * Get Zeppelin Git branch
   *
   * @return Latest Zeppelin commit id
   */
  static String getGitBranch() {
    return StringUtils.defaultIfEmpty(projectProperties.getProperty(BRANCH_NAME_PROPERTY),
        StringUtils.EMPTY);
  }

  /**
   * Get Zeppelin Git latest commit timestamp
   *
   * @return Latest Zeppelin commit timestamp
   */
  static String getBuildTimestamp() {
    return StringUtils.defaultIfEmpty(projectProperties.getProperty(BUILD_TIME_PROPERTY),
            StringUtils.EMPTY);
  }
}

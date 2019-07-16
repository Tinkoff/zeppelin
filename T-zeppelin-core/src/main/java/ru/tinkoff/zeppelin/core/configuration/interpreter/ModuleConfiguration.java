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

package ru.tinkoff.zeppelin.core.configuration.interpreter;

import ru.tinkoff.zeppelin.core.configuration.interpreter.option.Permissions;

import java.io.Serializable;

/**
 * Full interpreter settings on interpreter page.
 */
public class ModuleConfiguration implements Serializable {

  private long id;

  private String shebang;

  private String humanReadableName;

  private String bindedTo;

  private String jvmOptions;

  private int concurrentTasks;
  private int regularTTL;
  private int scheduledTTL;

  private long moduleInnerConfigId;

  private long moduleSourceId;

  private Permissions permissions;

  private boolean isEnabled;

  public ModuleConfiguration(final long id,
                             final String shebang,
                             final String humanReadableName,
                             final String bindedTo,
                             final String jvmOptions,
                             final int concurrentTasks,
                             final int regularTTL,
                             final int scheduledTTL,
                             final long moduleInnerConfigId,
                             final long moduleSourceId,
                             final Permissions permissions,
                             final boolean isEnabled) {
    this.id = id;
    this.shebang = shebang;
    this.humanReadableName = humanReadableName;
    this.bindedTo = bindedTo;
    this.jvmOptions = jvmOptions;
    this.concurrentTasks = concurrentTasks;
    this.regularTTL = regularTTL;
    this.scheduledTTL = scheduledTTL;
    this.moduleInnerConfigId = moduleInnerConfigId;
    this.moduleSourceId = moduleSourceId;
    this.permissions = permissions;
    this.isEnabled = isEnabled;
  }

  public long getId() {
    return id;
  }

  public void setId(final long id) {
    this.id = id;
  }

  public String getShebang() {
    return shebang;
  }

  public void setShebang(final String shebang) {
    this.shebang = shebang;
  }

  public String getHumanReadableName() {
    return humanReadableName;
  }

  public void setHumanReadableName(final String humanReadableName) {
    this.humanReadableName = humanReadableName;
  }

  public String getBindedTo() {
    return bindedTo;
  }

  public void setBindedTo(final String bindedTo) {
    this.bindedTo = bindedTo;
  }

  public String getJvmOptions() {
    return jvmOptions;
  }

  public void setJvmOptions(final String jvmOptions) {
    this.jvmOptions = jvmOptions;
  }

  public int getConcurrentTasks() {
    return concurrentTasks;
  }

  public void setConcurrentTasks(final int concurrentTasks) {
    this.concurrentTasks = concurrentTasks;
  }

  public int getRegularTTL() {
    return regularTTL;
  }

  public void setRegularTTL(int regularTTL) {
    this.regularTTL = regularTTL;
  }

  public int getScheduledTTL() {
    return scheduledTTL;
  }

  public void setScheduledTTL(int scheduledTTL) {
    this.scheduledTTL = scheduledTTL;
  }

  public long getModuleInnerConfigId() {
    return moduleInnerConfigId;
  }

  public void setModuleInnerConfigId(final long moduleInnerConfigId) {
    this.moduleInnerConfigId = moduleInnerConfigId;
  }

  public long getModuleSourceId() {
    return moduleSourceId;
  }

  public void setModuleSourceId(final long moduleSourceId) {
    this.moduleSourceId = moduleSourceId;
  }

  public Permissions getPermissions() {
    return permissions;
  }

  public void setPermissions(final Permissions permissions) {
    this.permissions = permissions;
  }

  public boolean isEnabled() {
    return isEnabled;
  }

  public void setEnabled(final boolean enabled) {
    isEnabled = enabled;
  }
}

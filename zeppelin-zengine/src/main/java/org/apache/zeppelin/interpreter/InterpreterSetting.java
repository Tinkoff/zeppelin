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

package org.apache.zeppelin.interpreter;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.zeppelin.dep.Dependency;
import com.google.gson.annotations.SerializedName;
import com.google.gson.internal.StringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import static org.apache.zeppelin.notebook.utility.IdHashes.generateId;

/**
 * Interpreter settings
 */
public class InterpreterSetting {

  private static final Logger logger = LoggerFactory.getLogger(InterpreterSetting.class);
  private static final String SHARED_PROCESS = "shared_process";
  private String id;
  private String name;
  // always be null in case of InterpreterSettingRef
  private String group;
  private transient Map<String, String> infos;

  /**
   * properties can be either Map<String, DefaultInterpreterProperty> or
   * Map<String, InterpreterProperty>
   * properties should be:
   * - Map<String, InterpreterProperty> when Interpreter instances are saved to
   * `conf/interpreter.json` file
   * - Map<String, DefaultInterpreterProperty> when Interpreters are registered
   * : this is needed after https://github.com/apache/zeppelin/pull/1145
   * which changed the way of getting default interpreter setting AKA interpreterSettingsRef
   */
  private Object properties;
  private Status status;
  private String errorReason;

  @SerializedName("interpreterGroup")
  private List<InterpreterInfo> interpreterInfos;
  private final transient Map<String, InterpreterGroup> interpreterGroupRef = new HashMap<>();
  private List<Dependency> dependencies = new LinkedList<>();
  private InterpreterOption option;
  private transient String path;

  @SerializedName("runner")
  private InterpreterRunner interpreterRunner;

  @Deprecated
  private transient InterpreterGroupFactory interpreterGroupFactory;

  private final transient ReentrantReadWriteLock.ReadLock interpreterGroupReadLock;
  private final transient ReentrantReadWriteLock.WriteLock interpreterGroupWriteLock;

  public InterpreterSetting() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    interpreterGroupReadLock = lock.readLock();
    interpreterGroupWriteLock = lock.writeLock();
  }

  public InterpreterSetting(String id, String name, String group,
      List<InterpreterInfo> interpreterInfos, Object properties, List<Dependency> dependencies,
      InterpreterOption option, String path, InterpreterRunner runner) {
    this();
    this.id = id;
    this.name = name;
    this.group = group;
    this.interpreterInfos = interpreterInfos;
    this.properties = properties;
    this.dependencies = dependencies;
    this.option = option;
    this.path = path;
    this.status = Status.READY;
    this.interpreterRunner = runner;
  }

  public InterpreterSetting(String name, String group, List<InterpreterInfo> interpreterInfos,
      Object properties, List<Dependency> dependencies, InterpreterOption option, String path,
      InterpreterRunner runner) {
    this(generateId(), name, group, interpreterInfos, properties, dependencies, option, path,
        runner);
  }

  /**
   * Create interpreter from interpreterSettingRef
   *
   * @param o interpreterSetting from interpreterSettingRef
   */
  public InterpreterSetting(InterpreterSetting o) {
    this(generateId(), o.getName(), o.getGroup(), o.getInterpreterInfos(), o.getProperties(),
        o.getDependencies(), o.getOption(), o.getPath(), o.getInterpreterRunner());
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  String getGroup() {
    return group;
  }

  private String getInterpreterProcessKey(String user, String noteId) {
    InterpreterOption option = getOption();
    String key;
    if (getOption().isExistingProcess) {
      key = Constants.EXISTING_PROCESS;
    } else if (getOption().isProcess()) {
      key = (option.perUserIsolated() ? user : "") + ":" + (option.perNoteIsolated() ? noteId : "");
    } else {
      key = SHARED_PROCESS;
    }

    //logger.debug("getInterpreterProcessKey: {} for InterpreterSetting Id: {}, Name: {}",
    //    key, getId(), getName());
    return key;
  }

  private boolean isEqualInterpreterKeyProcessKey(String refKey, String processKey) {
    InterpreterOption option = getOption();
    int validCount = 0;
    if (getOption().isProcess()
        && !(option.perUserIsolated() == true && option.perNoteIsolated() == true)) {

      List<String> processList = Arrays.asList(processKey.split(":"));
      List<String> refList = Arrays.asList(refKey.split(":"));

      if (refList.size() <= 1 || processList.size() <= 1) {
        return refKey.equals(processKey);
      }

      if (processList.get(0).equals("") || processList.get(0).equals(refList.get(0))) {
        validCount = validCount + 1;
      }

      if (processList.get(1).equals("") || processList.get(1).equals(refList.get(1))) {
        validCount = validCount + 1;
      }

      return (validCount >= 2);
    } else {
      return refKey.equals(processKey);
    }
  }

  String getInterpreterSessionKey(String user, String noteId) {
    InterpreterOption option = getOption();
    String key;
    if (option.isExistingProcess()) {
      key = Constants.EXISTING_PROCESS;
    } else if (option.perNoteScoped() && option.perUserScoped()) {
      key = user + ":" + noteId;
    } else if (option.perUserScoped()) {
      key = user;
    } else if (option.perNoteScoped()) {
      key = noteId;
    } else {
      key = "shared_session";
    }

    logger.debug("Interpreter session key: {}, for note: {}, user: {}, InterpreterSetting Name: " +
        "{}", key, noteId, user, getName());
    return key;
  }

  public InterpreterGroup getInterpreterGroup(String user, String noteId) {
    String key = getInterpreterProcessKey(user, noteId);
    if (!interpreterGroupRef.containsKey(key)) {
      String interpreterGroupId = getId() + ":" + key;
      InterpreterGroup intpGroup =
          interpreterGroupFactory.createInterpreterGroup(interpreterGroupId, getOption());

      interpreterGroupWriteLock.lock();
      logger.debug("create interpreter group with groupId:" + interpreterGroupId);
      interpreterGroupRef.put(key, intpGroup);
      interpreterGroupWriteLock.unlock();
    }
    try {
      interpreterGroupReadLock.lock();
      return interpreterGroupRef.get(key);
    } finally {
      interpreterGroupReadLock.unlock();
    }
  }

  public Collection<InterpreterGroup> getAllInterpreterGroups() {
    try {
      interpreterGroupReadLock.lock();
      return new LinkedList<>(interpreterGroupRef.values());
    } finally {
      interpreterGroupReadLock.unlock();
    }
  }

  void closeAndRemoveInterpreterGroup(String noteId, String user) {
    if (user.equals("anonymous")) {
      user = "";
    }
    String processKey = getInterpreterProcessKey(user, noteId);
    String sessionKey = getInterpreterSessionKey(user, noteId);
    List<InterpreterGroup> groupToRemove = new LinkedList<>();
    InterpreterGroup groupItem;
    for (String intpKey : new HashSet<>(interpreterGroupRef.keySet())) {
      if (isEqualInterpreterKeyProcessKey(intpKey, processKey)) {
        interpreterGroupWriteLock.lock();
        // TODO(jl): interpreterGroup has two or more sessionKeys inside it. thus we should not
        // remove interpreterGroup if it has two or more values.
        groupItem = interpreterGroupRef.get(intpKey);
        interpreterGroupWriteLock.unlock();
        groupToRemove.add(groupItem);
      }
      for (InterpreterGroup groupToClose : groupToRemove) {
        // TODO(jl): Fix the logic removing session. Now, it's handled into groupToClose.clsose()
        groupToClose.close(interpreterGroupRef, intpKey, sessionKey);
      }
      groupToRemove.clear();
    }

    //Remove session because all interpreters in this session are closed
    //TODO(jl): Change all code to handle interpreter one by one or all at once

  }

  void closeAndRemoveAllInterpreterGroups() {
    for (String processKey : new HashSet<>(interpreterGroupRef.keySet())) {
      InterpreterGroup interpreterGroup = interpreterGroupRef.get(processKey);
      for (String sessionKey : new HashSet<>(interpreterGroup.keySet())) {
        interpreterGroup.close(interpreterGroupRef, processKey, sessionKey);
      }
    }
  }

  void shutdownAndRemoveAllInterpreterGroups() {
    for (InterpreterGroup interpreterGroup : interpreterGroupRef.values()) {
      interpreterGroup.shutdown();
    }
  }

  public Object getProperties() {
    return properties;
  }

  public Properties getFlatProperties() {
    Properties p = new Properties();
    if (properties != null) {
      Map<String, InterpreterProperty> propertyMap = (Map<String, InterpreterProperty>) properties;
      for (String key : propertyMap.keySet()) {
        InterpreterProperty tmp = propertyMap.get(key);
        p.put(tmp.getName() != null ? tmp.getName() : key,
            tmp.getValue() != null ? tmp.getValue().toString() : null);
      }
    }
    return p;
  }

  public List<Dependency> getDependencies() {
    if (dependencies == null) {
      return new LinkedList<>();
    }
    return dependencies;
  }

  public void setDependencies(List<Dependency> dependencies) {
    this.dependencies = dependencies;
  }

  public InterpreterOption getOption() {
    if (option == null) {
      option = new InterpreterOption();
    }

    return option;
  }

  public void setOption(InterpreterOption option) {
    this.option = option;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public List<InterpreterInfo> getInterpreterInfos() {
    return interpreterInfos;
  }

  void setInterpreterGroupFactory(InterpreterGroupFactory interpreterGroupFactory) {
    this.interpreterGroupFactory = interpreterGroupFactory;
  }

  void appendDependencies(List<Dependency> dependencies) {
    for (Dependency dependency : dependencies) {
      if (!this.dependencies.contains(dependency)) {
        this.dependencies.add(dependency);
      }
    }
  }

  void setInterpreterOption(InterpreterOption interpreterOption) {
    this.option = interpreterOption;
  }

  public void setProperties(Map<String, InterpreterProperty> p) {
    this.properties = p;
  }

  void setGroup(String group) {
    this.group = group;
  }

  void setName(String name) {
    this.name = name;
  }

  /***
   * Interpreter status
   */
  public enum Status {
    DOWNLOADING_DEPENDENCIES,
    ERROR,
    READY
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public String getErrorReason() {
    return errorReason;
  }

  public void setErrorReason(String errorReason) {
    this.errorReason = errorReason;
  }

  public void setInfos(Map<String, String> infos) {
    this.infos = infos;
  }

  public Map<String, String> getInfos() {
    return infos;
  }

  public InterpreterRunner getInterpreterRunner() {
    return interpreterRunner;
  }

  public void setInterpreterRunner(InterpreterRunner interpreterRunner) {
    this.interpreterRunner = interpreterRunner;
  }

  // For backward compatibility of interpreter.json format after ZEPPELIN-2654
  public void convertPermissionsFromUsersToOwners(JsonObject jsonObject) {
    if (jsonObject != null) {
      JsonObject option = jsonObject.getAsJsonObject("option");
      if (option != null) {
        JsonArray users = option.getAsJsonArray("users");
        if (users != null) {
          if (this.option.getOwners() == null) {
            this.option.owners = new LinkedList<>();
          }
          for (JsonElement user : users) {
            this.option.getOwners().add(user.getAsString());
          }
        }
      }
    }
  }

  // For backward compatibility of interpreter.json format after ZEPPELIN-2403
  public void convertFlatPropertiesToPropertiesWithWidgets() {
    StringMap newProperties = new StringMap();
    if (properties != null && properties instanceof StringMap) {
      StringMap p = (StringMap) properties;

      for (Object o : p.entrySet()) {
        Map.Entry entry = (Map.Entry) o;
        if (!(entry.getValue() instanceof StringMap)) {
          StringMap newProperty = new StringMap();
          newProperty.put("name", entry.getKey());
          newProperty.put("value", entry.getValue());
          newProperty.put("type", InterpreterPropertyType.TEXTAREA.getValue());
          newProperties.put(entry.getKey().toString(), newProperty);
        } else {
          // already converted
          return;
        }
      }

      this.properties = newProperties;
    }
  }
}

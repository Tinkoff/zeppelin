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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.Dependency;
import org.apache.zeppelin.DependencyResolver;
import org.apache.zeppelin.configuration.ZeppelinConfiguration;
import org.apache.zeppelin.interpreterV2.ApplicationEventListener;
import org.apache.zeppelin.interpreter.Interpreter.RegisteredInterpreter;
import org.apache.zeppelin.interpreter.remote.RemoteAngularObjectRegistry;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcessListener;
import org.apache.zeppelin.interpreterV2.server.InterpreterProcessServer;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.notebook.display.AngularObjectRegistry;
import org.apache.zeppelin.notebook.display.AngularObjectRegistryListener;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.eclipse.jetty.util.annotation.ManagedAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sonatype.aether.repository.Authentication;
import org.sonatype.aether.repository.Proxy;
import org.sonatype.aether.repository.RemoteRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.*;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileSystem;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;


/**
 * InterpreterSettingManager is the component which manage all the interpreter settings.
 * (load/create/update/remove/get)
 * TODO(zjffdu) We could move it into another separated component.
 */
@Component
public class InterpreterSettingManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(InterpreterSettingManager.class);
  private static final Map<String, Object> DEFAULT_EDITOR = ImmutableMap.of(
      "language", (Object) "text",
      "editOnDblClick", false);
  private static final String FILENAME = "/interpreter.json";

  private final ZeppelinConfiguration conf;
  private final Path interpreterDirPath;

  /**
   * This is only InterpreterSetting templates with default name and properties
   * name --> InterpreterSetting
   */
  private final Map<String, InterpreterSetting> interpreterSettingTemplates =
      Maps.newConcurrentMap();
  /**
   * This is used by creating and running Interpreters
   * id --> InterpreterSetting
   * TODO(zjffdu) change it to name --> InterpreterSetting
   */
  private final Map<String, InterpreterSetting> interpreterSettings = Maps.newConcurrentMap();

  private final List<RemoteRepository> interpreterRepositories;
  private InterpreterOption defaultOption;
  private String defaultInterpreterGroup;
  private final Gson gson;

  private RemoteInterpreterProcessListener remoteInterpreterProcessListener;
  private ApplicationEventListener appEventListener;
  private DependencyResolver dependencyResolver;

  @Lazy
  @Autowired
  private InterpreterProcessServer interpreterEventServer;

  @Lazy
  @Autowired
  private AngularObjectRegistryListener angularObjectRegistryListener;

  @Autowired
  public InterpreterSettingManager(ZeppelinConfiguration zeppelinConfiguration,
      @Qualifier("ApplicationEventListenerImpl") ApplicationEventListener appEventListener)
      throws IOException {
    this(zeppelinConfiguration, new InterpreterOption(), appEventListener);
  }

  public InterpreterSettingManager(ZeppelinConfiguration conf,
      InterpreterOption defaultOption,
      ApplicationEventListener appEventListener)
      throws IOException {
    this.conf = conf;
    this.defaultOption = defaultOption;
    this.interpreterDirPath = Paths.get(conf.getInterpreterDir());
    LOGGER.debug("InterpreterRootPath: {}", interpreterDirPath);
    this.dependencyResolver = new DependencyResolver(conf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_LOCALREPO));
    this.interpreterRepositories = dependencyResolver.getRepos();
    this.defaultInterpreterGroup = conf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_GROUP_DEFAULT);
    this.gson = new GsonBuilder().setPrettyPrinting().create();

    this.appEventListener = appEventListener;

    this.interpreterEventServer = new InterpreterProcessServer(conf, this);
    this.interpreterEventServer.start();
    init();
  }

  public void refreshInterpreterTemplates() {
    Set<String> installedInterpreters = Sets.newHashSet(interpreterSettingTemplates.keySet());

    try {
      LOGGER.info("Refreshing interpreter list");
      loadInterpreterSettingFromDefaultDir(false);
      Set<String> newlyAddedInterpreters = Sets.newHashSet(interpreterSettingTemplates.keySet());
      newlyAddedInterpreters.removeAll(installedInterpreters);
      if(!newlyAddedInterpreters.isEmpty()) {
        saveToFile();
      }
    } catch (IOException e) {
      LOGGER.error("Error while saving interpreter settings.");
    }
  }


  private void initInterpreterSetting(InterpreterSetting interpreterSetting) {
    interpreterSetting.setConf(conf)
        .setInterpreterSettingManager(this)
        .setAngularObjectRegistryListener(angularObjectRegistryListener)
        .setRemoteInterpreterProcessListener(remoteInterpreterProcessListener)
        .setAppEventListener(appEventListener)
        .setDependencyResolver(dependencyResolver)
        .setInterpreterEventServer(interpreterEventServer)
        .postProcessing();
  }

  /**
   * Load interpreter setting from interpreter.json
   */
  private void loadFromFile() throws IOException {
    File file = new File(conf.getConfigFSDir() + FILENAME);
    InterpreterInfoSaving infoSaving = null;
    try {
      String json = IOUtils.toString(new FileInputStream(file));
      LOGGER.info("Load Interpreter Setting from file: " + file);
      JsonObject jsonObject = new JsonParser().parse(json).getAsJsonObject();
      infoSaving = new Gson().fromJson(json, InterpreterInfoSaving.class);
      for (InterpreterSetting interpreterSetting : infoSaving.interpreterSettings.values()) {
        interpreterSetting.convertPermissionsFromUsersToOwners(
            jsonObject.getAsJsonObject("interpreterSettings")
                .getAsJsonObject(interpreterSetting.getId()));
      }
    } catch (IOException e) {
      LOGGER.warn("Interpreter Setting file {} is not existed", file);
    }

    if (infoSaving == null) {
      // it is fresh zeppelin instance if there's no interpreter.json, just create interpreter
      // setting from interpreterSettingTemplates
      for (InterpreterSetting interpreterSettingTemplate : interpreterSettingTemplates.values()) {
        InterpreterSetting interpreterSetting = new InterpreterSetting(interpreterSettingTemplate);
        initInterpreterSetting(interpreterSetting);
        interpreterSettings.put(interpreterSetting.getId(), interpreterSetting);
      }
      return;
    }

    //TODO(zjffdu) still ugly (should move all to InterpreterInfoSaving)
    for (InterpreterSetting savedInterpreterSetting : infoSaving.interpreterSettings.values()) {
      savedInterpreterSetting.setProperties(InterpreterSetting.convertInterpreterProperties(
          savedInterpreterSetting.getProperties()
      ));
      initInterpreterSetting(savedInterpreterSetting);

      InterpreterSetting interpreterSettingTemplate =
          interpreterSettingTemplates.get(savedInterpreterSetting.getGroup());
      // InterpreterSettingTemplate is from interpreter-setting.json which represent the latest
      // InterpreterSetting, while InterpreterSetting is from interpreter.json which represent
      // the user saved interpreter setting
      if (interpreterSettingTemplate != null) {
        savedInterpreterSetting.setInterpreterDir(interpreterSettingTemplate.getInterpreterDir());
        // merge properties from interpreter-setting.json and interpreter.json
        Map<String, InterpreterProperty> mergedProperties =
            new HashMap<>(InterpreterSetting.convertInterpreterProperties(
                interpreterSettingTemplate.getProperties()));
        Map<String, InterpreterProperty> savedProperties = InterpreterSetting
            .convertInterpreterProperties(savedInterpreterSetting.getProperties());
        for (Map.Entry<String, InterpreterProperty> entry : savedProperties.entrySet()) {
          // only merge properties whose value is not empty
          if (entry.getValue().getValue() != null && !
              StringUtils.isBlank(entry.getValue().toString())) {
            mergedProperties.put(entry.getKey(), entry.getValue());
          }
        }
        savedInterpreterSetting.setProperties(mergedProperties);
        // merge InterpreterInfo
        savedInterpreterSetting.setInterpreterInfos(
            interpreterSettingTemplate.getInterpreterInfos());
        savedInterpreterSetting.setInterpreterRunner(
            interpreterSettingTemplate.getInterpreterRunner());
      } else {
        LOGGER.warn("No InterpreterSetting Template found for InterpreterSetting: "
            + savedInterpreterSetting.getGroup() + ", but it is found in interpreter.json, "
            + "it would be skipped.");
        continue;
      }

      // Overwrite the default InterpreterSetting we registered from InterpreterSetting Templates
      // remove it first
      for (InterpreterSetting setting : interpreterSettings.values()) {
        if (setting.getName().equals(savedInterpreterSetting.getName())) {
          interpreterSettings.remove(setting.getId());
        }
      }
      savedInterpreterSetting.postProcessing();
      LOGGER.info("Create Interpreter Setting {} from interpreter.json",
          savedInterpreterSetting.getName());
      interpreterSettings.put(savedInterpreterSetting.getId(), savedInterpreterSetting);
    }

    if (infoSaving.interpreterRepositories != null) {
      for (RemoteRepository repo : infoSaving.interpreterRepositories) {
        if (!dependencyResolver.getRepos().contains(repo)) {
          this.interpreterRepositories.add(repo);
        }
      }

      // force interpreter dependencies loading once the
      // repositories have been loaded.
      for (InterpreterSetting setting : interpreterSettings.values()) {
        setting.setDependencies(setting.getDependencies());
      }
    }
  }

  @SuppressWarnings("Duplicates")
  public void saveToFile() throws IOException {
    InterpreterInfoSaving info = new InterpreterInfoSaving();
    info.interpreterSettings = Maps.newHashMap(interpreterSettings);
    info.interpreterRepositories = interpreterRepositories;
    File tempFile = null;
    try {
      File file = new File(conf.getConfigFSDir() + FILENAME);
      File directory = file.getParentFile();
      directory.mkdirs();
      tempFile = File.createTempFile(file.getName(), null, directory);
      FileOutputStream out = new FileOutputStream(tempFile);
      IOUtils.write(new Gson().toJson(info), out );
      out.close();
      FileSystem defaultFileSystem = FileSystems.getDefault();
      Path tempFilePath = defaultFileSystem.getPath(tempFile.getCanonicalPath());
      Path destinationFilePath = defaultFileSystem.getPath(file.getCanonicalPath());
      Files.move(tempFilePath, destinationFilePath, StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      LOGGER.error("Error saving notebook authorization file", e);
    } finally {
      if (tempFile != null) {
        tempFile.delete();
      }
    }
  }

  private void init() throws IOException {

    loadInterpreterSettingFromDefaultDir(true);
    loadFromFile();
    //saveToFile();
  }

  private void loadInterpreterSettingFromDefaultDir(boolean override) throws IOException {
    // 1. detect interpreter setting via interpreter-setting.json in each interpreter folder
    // 2. detect interpreter setting in interpreter.json that is saved before
    String interpreterJson = conf.getInterpreterJson();
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (Files.exists(interpreterDirPath)) {
      for (Path interpreterDir : Files
          .newDirectoryStream(interpreterDirPath, new Filter<Path>() {
            @Override
            public boolean accept(Path entry) throws IOException {
              return Files.exists(entry) && Files.isDirectory(entry);
            }
          })) {
        String interpreterDirString = interpreterDir.toString();
        /**
         * Register interpreter by the following ordering
         * 1. Register it from path {ZEPPELIN_HOME}/interpreter/{interpreter_name}/
         *    interpreter-setting.json
         * 2. Register it from interpreter-setting.json in classpath
         *    {ZEPPELIN_HOME}/interpreter/{interpreter_name}
         */
        if (!registerInterpreterFromPath(interpreterDirString, interpreterJson, override)) {
          if (!registerInterpreterFromResource(cl, interpreterDirString, interpreterJson,
              override)) {
            LOGGER.warn("No interpreter-setting.json found in " + interpreterDirString);
          }
        }
      }
    } else {
      LOGGER.warn("InterpreterDir {} doesn't exist", interpreterDirPath);
    }
  }

  public RemoteInterpreterProcessListener getRemoteInterpreterProcessListener() {
    return remoteInterpreterProcessListener;
  }

  public ApplicationEventListener getAppEventListener() {
    return appEventListener;
  }

  private boolean registerInterpreterFromResource(ClassLoader cl, String interpreterDir,
      String interpreterJson, boolean override) throws IOException {
    URL[] urls = recursiveBuildLibList(new File(interpreterDir));
    ClassLoader tempClassLoader = new URLClassLoader(urls, null);

    URL url = tempClassLoader.getResource(interpreterJson);
    if (url == null) {
      return false;
    }

    LOGGER.debug("Reading interpreter-setting.json from {} as Resource", url);
    List<RegisteredInterpreter> registeredInterpreterList =
        getInterpreterListFromJson(url.openStream());
    registerInterpreterSetting(registeredInterpreterList, interpreterDir, override);
    return true;
  }

  private boolean registerInterpreterFromPath(String interpreterDir, String interpreterJson,
      boolean override) throws IOException {

    Path interpreterJsonPath = Paths.get(interpreterDir, interpreterJson);
    if (Files.exists(interpreterJsonPath)) {
      LOGGER.debug("Reading interpreter-setting.json from file {}", interpreterJsonPath);
      List<RegisteredInterpreter> registeredInterpreterList =
          getInterpreterListFromJson(new FileInputStream(interpreterJsonPath.toFile()));
      registerInterpreterSetting(registeredInterpreterList, interpreterDir, override);
      return true;
    }
    return false;
  }

  private List<RegisteredInterpreter> getInterpreterListFromJson(InputStream stream) {
    Type registeredInterpreterListType = new TypeToken<List<RegisteredInterpreter>>() {
    }.getType();
    return gson.fromJson(new InputStreamReader(stream), registeredInterpreterListType);
  }

  private void registerInterpreterSetting(List<RegisteredInterpreter> registeredInterpreters, String interpreterDir, boolean override) {

    Map<String, DefaultInterpreterProperty> properties = new HashMap<>();
    List<InterpreterInfo> interpreterInfos = new ArrayList<>();
    InterpreterOption option = defaultOption;
    String group = null;
    InterpreterRunner runner = null;
    for (RegisteredInterpreter registeredInterpreter : registeredInterpreters) {
      //TODO(zjffdu) merge RegisteredInterpreter & InterpreterInfo
      InterpreterInfo interpreterInfo =
          new InterpreterInfo(registeredInterpreter.getClassName(), registeredInterpreter.getName(),
              registeredInterpreter.isDefaultInterpreter(), registeredInterpreter.getEditor());
      group = registeredInterpreter.getGroup();
      runner = registeredInterpreter.getRunner();
      // use defaultOption if it is not specified in interpreter-setting.json
      if (registeredInterpreter.getOption() != null) {
        option = registeredInterpreter.getOption();
      }
      properties.putAll(registeredInterpreter.getProperties());
      interpreterInfos.add(interpreterInfo);
    }

    InterpreterSetting interpreterSettingTemplate = new InterpreterSetting.Builder()
        .setGroup(group)
        .setName(group)
        .setInterpreterInfos(interpreterInfos)
        .setProperties(properties)
        .setDependencies(new ArrayList<Dependency>())
        .setOption(option)
        .setRunner(runner)
        .setInterpreterDir(interpreterDir)
        .setRunner(runner)
        .setConf(conf)
        .setIntepreterSettingManager(this)
        .create();

    String key = interpreterSettingTemplate.getName();
    if(override || !interpreterSettingTemplates.containsKey(key)) {
      LOGGER.info("Register InterpreterSettingTemplate: {}", key);
      interpreterSettingTemplates.put(key, interpreterSettingTemplate);
    }
  }

  @VisibleForTesting
  public InterpreterSetting getDefaultInterpreterSetting(String noteId) {
    List<InterpreterSetting> allInterpreterSettings = getInterpreterSettings(noteId);
    return allInterpreterSettings.size() > 0 ? allInterpreterSettings.get(0) : null;
  }

  public List<InterpreterSetting> getInterpreterSettings(String noteId) {
    return get();
  }

  public InterpreterSetting getInterpreterSettingByName(String name) {
    try {
      for (InterpreterSetting setting : interpreterSettings.values()) {
        if (setting.getName().equals(name)) {
          return setting;
        }
      }
      throw new RuntimeException("No such interpreter setting: " + name);
    } finally {
    }
  }

  public ManagedInterpreterGroup getInterpreterGroupById(String groupId) {
    for (InterpreterSetting setting : interpreterSettings.values()) {
      ManagedInterpreterGroup interpreterGroup = setting.getInterpreterGroup(groupId);
      if (interpreterGroup != null) {
        return interpreterGroup;
      }
    }
    return null;
  }

  //TODO(zjffdu) logic here is a little ugly
  public Map<String, Object> getEditorSetting(Interpreter interpreter, String user, String noteId,
      String replName) {
    Map<String, Object> editor = DEFAULT_EDITOR;
    String group = StringUtils.EMPTY;
    try {
      String defaultSettingName = getDefaultInterpreterSetting(noteId).getName();
      List<InterpreterSetting> intpSettings = getInterpreterSettings(noteId);
      for (InterpreterSetting intpSetting : intpSettings) {
        String[] replNameSplit = replName.split("\\.");
        if (replNameSplit.length == 2) {
          group = replNameSplit[0];
        }
        // when replName is 'name' of interpreter
        if (intpSetting.getName().equals(defaultSettingName)) {
          editor = intpSetting.getEditorFromSettingByClassName(interpreter.getClassName());
        }
        // when replName is 'alias name' of interpreter or 'group' of interpreter
        if (replName.equals(intpSetting.getName()) || group.equals(intpSetting.getName())) {
          editor = intpSetting.getEditorFromSettingByClassName(interpreter.getClassName());
          break;
        }
      }
    } catch (NullPointerException e) {
      // Use `debug` level because this log occurs frequently
      LOGGER.debug("Couldn't get interpreter editor setting");
    }
    return editor;
  }

  public List<ManagedInterpreterGroup> getAllInterpreterGroup() {
    List<ManagedInterpreterGroup> interpreterGroups = new ArrayList<>();
    for (InterpreterSetting interpreterSetting : interpreterSettings.values()) {
      interpreterGroups.addAll(interpreterSetting.getAllInterpreterGroups());
    }
    return interpreterGroups;
  }

  /**
   * Overwrite dependency jar under local-repository/{interpreterId} if jar file in original path is
   * changed
   */
  private void copyDependenciesFromLocalPath(final InterpreterSetting setting) {
    setting.setStatus(InterpreterSetting.Status.DOWNLOADING_DEPENDENCIES);
    final Thread t = new Thread() {
      public void run() {
        try {
          List<Dependency> deps = setting.getDependencies();
          if (deps != null) {
            for (Dependency d : deps) {
              File destDir = new File(
                  conf.getRelativeDir(ZeppelinConfiguration.ConfVars.ZEPPELIN_DEP_LOCALREPO));

              int numSplits = d.getGroupArtifactVersion().split(":").length;
              if (!(numSplits >= 3 && numSplits <= 6)) {
                dependencyResolver.copyLocalDependency(d.getGroupArtifactVersion(),
                    new File(destDir, setting.getId()));
              }
            }
          }
          setting.setStatus(InterpreterSetting.Status.READY);
        } catch (Exception e) {
          LOGGER.error(String.format("Error while copying deps for interpreter group : %s," +
                  " go to interpreter setting page click on edit and save it again to make " +
                  "this interpreter work properly.",
              setting.getGroup()), e);
          setting.setErrorReason(e.getLocalizedMessage());
          setting.setStatus(InterpreterSetting.Status.ERROR);
        } finally {

        }
      }
    };
    t.start();
  }

  /**
   * Return ordered interpreter setting list.
   * The list does not contain more than one setting from the same interpreter class.
   * Order by InterpreterClass (order defined by ZEPPELIN_INTERPRETERS), Interpreter setting name
   */
  public List<String> getInterpreterSettingIds() {
    List<String> settingIdList = new ArrayList<>();
    for (InterpreterSetting interpreterSetting : get()) {
      settingIdList.add(interpreterSetting.getId());
    }
    return settingIdList;
  }

  public InterpreterSetting createNewSetting(String name, String group,
      List<Dependency> dependencies, InterpreterOption option, Map<String, InterpreterProperty> p)
      throws IOException {

    if (name.indexOf(".") >= 0) {
      throw new IOException("'.' is invalid for InterpreterSetting name.");
    }
    // check if name is existed
    for (InterpreterSetting interpreterSetting : interpreterSettings.values()) {
      if (interpreterSetting.getName().equals(name)) {
        throw new IOException("Interpreter " + name + " already existed");
      }
    }
    InterpreterSetting setting = new InterpreterSetting(interpreterSettingTemplates.get(group));
    setting.setName(name);
    setting.setGroup(group);
    //TODO(zjffdu) Should use setDependencies
    setting.appendDependencies(dependencies);
    setting.setInterpreterOption(option);
    setting.setProperties(p);
    initInterpreterSetting(setting);
    interpreterSettings.put(setting.getId(), setting);
    saveToFile();
    return setting;
  }



  @VisibleForTesting
  public void closeNote(String user, String noteId) {
    // close interpreters in this note session
    LOGGER.info("Close Note: {}", noteId);
    List<InterpreterSetting> settings = getInterpreterSettings(noteId);
    for (InterpreterSetting setting : settings) {
      setting.closeInterpreters(user, noteId);
    }
  }

  public Map<String, InterpreterSetting> getInterpreterSettingTemplates() {
    return interpreterSettingTemplates;
  }

  private URL[] recursiveBuildLibList(File path) throws MalformedURLException {
    URL[] urls = new URL[0];
    if (path == null || !path.exists()) {
      return urls;
    } else if (path.getName().startsWith(".")) {
      return urls;
    } else if (path.isDirectory()) {
      File[] files = path.listFiles();
      if (files != null) {
        for (File f : files) {
          urls = (URL[]) ArrayUtils.addAll(urls, recursiveBuildLibList(f));
        }
      }
      return urls;
    } else {
      return new URL[]{path.toURI().toURL()};
    }
  }

  public List<RemoteRepository> getRepositories() {
    return this.interpreterRepositories;
  }

  public void addRepository(String id, String url, boolean snapshot, Authentication auth,
      Proxy proxy) throws IOException {
    dependencyResolver.addRepo(id, url, snapshot, auth, proxy);
    saveToFile();
  }

  public void removeRepository(String id) throws IOException {
    dependencyResolver.delRepo(id);
    saveToFile();
  }

  /** Change interpreter properties and restart */
  public void setPropertyAndRestart(
      String id,
      InterpreterOption option,
      Map<String, InterpreterProperty> properties,
      List<Dependency> dependencies)
      throws InterpreterException, IOException {
    InterpreterSetting intpSetting = interpreterSettings.get(id);
    if (intpSetting != null) {
      try {
        intpSetting.close();
        intpSetting.setOption(option);
        intpSetting.setProperties(properties);
        intpSetting.setDependencies(dependencies);
        intpSetting.postProcessing();
        saveToFile();
      } catch (Exception e) {
        loadFromFile();
        throw new IOException(e);
      }
    } else {
      throw new InterpreterException("Interpreter setting id " + id + " not found");
    }
  }

  // restart in note page
  public void restart(String settingId, String noteId, String user) throws InterpreterException {
    InterpreterSetting intpSetting = interpreterSettings.get(settingId);
    Preconditions.checkNotNull(intpSetting);
    intpSetting = interpreterSettings.get(settingId);
    // Check if dependency in specified path is changed
    // If it did, overwrite old dependency jar with new one
    if (intpSetting != null) {
      copyDependenciesFromLocalPath(intpSetting);
      intpSetting.closeInterpreters(user, noteId);
    } else {
      throw new InterpreterException("Interpreter setting id " + settingId + " not found");
    }
  }

  public void restart(String id) throws InterpreterException {
    InterpreterSetting setting = interpreterSettings.get(id);
    copyDependenciesFromLocalPath(setting);
    setting.close();
  }

  public InterpreterSetting get(String id) {
    return interpreterSettings.get(id);
  }

  @VisibleForTesting
  public InterpreterSetting getByName(String name) {
    for (InterpreterSetting interpreterSetting : interpreterSettings.values()) {
      if (interpreterSetting.getName().equals(name)) {
        return interpreterSetting;
      }
    }
    return null;
  }

  public void remove(String id) throws IOException {
    // 1. close interpreter groups of this interpreter setting
    // 2. remove this interpreter setting
    // 3. remove this interpreter setting from note binding
    // 4. clean local repository directory
    LOGGER.info("Remove interpreter setting: " + id);
    if (interpreterSettings.containsKey(id)) {
      InterpreterSetting intp = interpreterSettings.get(id);
      intp.close();
      interpreterSettings.remove(id);
      saveToFile();
    }

    File localRepoDir = new File(conf.getInterpreterLocalRepoPath() + "/" + id);
    FileUtils.deleteDirectory(localRepoDir);
  }

  /**
   * Get interpreter settings
   */
  public List<InterpreterSetting> get() {
    List<InterpreterSetting> orderedSettings = new ArrayList<>(interpreterSettings.values());
    Collections.sort(orderedSettings, new Comparator<InterpreterSetting>() {
      @Override
      public int compare(InterpreterSetting o1, InterpreterSetting o2) {
        if (o1.getName().equals(defaultInterpreterGroup)) {
          return -1;
        } else if (o2.getName().equals(defaultInterpreterGroup)) {
          return 1;
        } else {
          return o1.getName().compareTo(o2.getName());
        }
      }
    });
    return orderedSettings;
  }

  public InterpreterSetting getDefaultInterpreterSetting() {
    InterpreterSetting setting =
        getByName(conf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_GROUP_DEFAULT));
    if (setting != null) {
      return setting;
    } else {
      return get().get(0);
    }
  }

  @VisibleForTesting
  public List<String> getSettingIds() {
    List<String> settingIds = new ArrayList<>();
    for (InterpreterSetting interpreterSetting : get()) {
      settingIds.add(interpreterSetting.getId());
    }
    return settingIds;
  }

  public void close(String settingId) {
    get(settingId).close();
  }

  public void close() {
    List<Thread> closeThreads = interpreterSettings.values().stream()
            .map(intpSetting-> new Thread(intpSetting::close, intpSetting.getId() + "-close"))
            .peek(t -> t.setUncaughtExceptionHandler((th, e) ->
                    LOGGER.error("interpreterGroup close error", e)))
            .peek(Thread::start)
            .collect(Collectors.toList());

    for (Thread t : closeThreads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        LOGGER.error("Can't wait close interpreterGroup threads", e);
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  public Map<String, String> extractProcessInfo(ManagedInterpreterGroup mig) {
    HashMap<String, String> info = new HashMap<>();
    info.put("name", mig.getId());
    info.put("group", mig.getInterpreterSetting().getGroup());
    info.put("host", mig.getInterpreterProcess().getHost());
    info.put("port", String.valueOf(mig.getInterpreterProcess().getPort()));
    return info;
  }

  @ManagedAttribute
  public List<Map<String, String>> getRunningInterpretersInfo() {
    return interpreterSettings.values().stream()
        .map(InterpreterSetting::getAllInterpreterGroups)
        .flatMap(Collection::stream)
        .filter(mig -> mig.getInterpreterProcess() != null)
        .map(this::extractProcessInfo)
        .collect(Collectors.toList());
  }

  public void removeNote(Note note, AuthenticationInfo subject) throws IOException {
    // remove from all interpreter instance's angular object registry
    for (InterpreterSetting settings : interpreterSettings.values()) {
      InterpreterGroup interpreterGroup = settings.getInterpreterGroup(subject.getUser(), note.getId());
      if (interpreterGroup != null) {
        AngularObjectRegistry registry = interpreterGroup.getAngularObjectRegistry();
        if (registry instanceof RemoteAngularObjectRegistry) {
          // remove paragraph scope object
          for (Paragraph p : note.getParagraphs()) {
            ((RemoteAngularObjectRegistry) registry).removeAllAndNotifyRemoteProcess(note.getId(), p.getId());

            //TODO(egorklimov): application state removed from paragraph

            // remove app scope object
            //            List<ApplicationState> appStates = p.getAllApplicationStates();
            //            if (appStates != null) {
            //              for (ApplicationState app : appStates) {
            //                ((RemoteAngularObjectRegistry) registry)
            //                    .removeAllAndNotifyRemoteProcess(note.getId(), app.getId());
            //              }
            //            }
          }
          // remove note scope object
          ((RemoteAngularObjectRegistry) registry).removeAllAndNotifyRemoteProcess(note.getId(), null);
        } else {
          // remove paragraph scope object
          for (Paragraph p : note.getParagraphs()) {
            registry.removeAll(note.getId(), p.getId());

            //TODO(egorklimov): application state removed from paragraph

            // remove app scope object
            //            List<ApplicationState> appStates = p.getAllApplicationStates();
            //            if (appStates != null) {
            //              for (ApplicationState app : appStates) {
            //                registry.removeAll(note.getId(), app.getId());
            //              }
            //            }
          }
          // remove note scope object
          registry.removeAll(note.getId(), null);
        }
      }
    }
  }
}

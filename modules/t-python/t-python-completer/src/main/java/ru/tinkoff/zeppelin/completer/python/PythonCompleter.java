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

package ru.tinkoff.zeppelin.completer.python;

import com.google.gson.Gson;
import jep.JepException;
import org.apache.commons.lang3.StringUtils;
import ru.tinkoff.zeppelin.interpreter.Completer;
import ru.tinkoff.zeppelin.interpreter.InterpreterCompletion;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("unused")
public class PythonCompleter extends Completer {

  private static final String LIBRARY_NAME = "jep.so";

  private final static AtomicBoolean libraryIsLoaded = new AtomicBoolean(false);
  private JepInThread jep;
  private final Gson gson = new Gson();
  private static NoteContextLoader pyContext;

  private String autoimportedModules = "";

  @Override
  public String complete(
      String st,
      int cursorPosition,
      final Map<String, String> noteContext,
      final Map<String, String> userContext,
      final Map<String, String> configuration) {
    final Set<InterpreterCompletion> completions = new HashSet<>();
    try {
      int row = 1;
      int column = 0;
      final String pyContextInitScript = pyContext.getInitScript(noteContext.get("Z_ENV_NOTE_UUID"));
      st = autoimportedModules + pyContextInitScript + st;
      cursorPosition += autoimportedModules.length() + pyContextInitScript.length();

      // find row and column of cursor
      for (final String line : st.split("\n")) {
        final int length = line.length();
        if (length >= cursorPosition) {
          column = cursorPosition;
          break;
        }
        cursorPosition -= length + 1;
        row++;
      }

      final List<Map<String, String>> parsedCompletions;
      //noinspection SynchronizeOnNonFinalField
      synchronized (jep) {
        jep.set("payload", st);
        jep.set("row", row);
        jep.set("column", column);
        jep.eval("script = jedi.Script(payload, row, column, '', sys_path=INCLUDE_PATHS_JEDI_ENV)");
        //noinspection unchecked
        parsedCompletions = (List<Map<String, String>>) jep.getValue("parse_completions(script.completions())");
      }

      for (final Map<String, String> comp : parsedCompletions) {
        final String type = comp.get("type");
        final String description = comp.get("description") + "\n" + comp.get("docString");
        String value = comp.get("name");
        String name = value;

        if ("function".equals(type)) {
          name += "(..)";
          value += "(";
        }

        final int score = getScore(type, comp.get("fullName"));

        completions.add(new InterpreterCompletion(name, value, type, description, score));
      }
      return gson.toJson(completions);
    } catch (final Throwable e) {
      return gson.toJson(Collections.emptySet());
    }
  }

  private int getScore(final String type, final String fullName) {
    int score = 10;

    // the higher the type, the higher the priority.
    switch (type) {
      case "statement":
        score += 10;
      case "function":
        score += 10;
      case "keyword":
        score += 10;
      case "class":
        score += 10;
      case "module":
        score += 10;
    }

    // if it local increase priority
    if (fullName.startsWith("__main__")) score += 1;

    return score;
  }

  @Override
  public void open(final Map<String, String> configuration, final String classPath) {

    // load Jep library
    synchronized (Completer.class) {
      try {
        if (!libraryIsLoaded.get()) {
          //TODO(SAN) not work correctly. Need fix it.
          loadJepLibrary(configuration);
          libraryIsLoaded.set(true);
        }
      } catch (final Throwable e) {
        //throw new RuntimeException("Can't load jep library", e);
      }
      if (pyContext == null) {
        pyContext = new NoteContextLoader(configuration.get("python.env.cache.folder"));
      }
    }

    // create Jep instance
    try {
      jep = getCustomizedJepInstance(configuration);
    } catch (final JepException e) {
      throw new RuntimeException("Can't create jep instance", e);
    }

    // init autoimported modules script
    final String modulesString = configuration.get("python.autoimport");
    for (final String module : modulesString.split(";")) {
      final String[] moduleProp = module.split(":");
      final String moduleName = moduleProp[0];
      final String moduleAlias = moduleProp.length > 1 ? moduleProp[1] : moduleName;
      if (moduleName.length() == 0 || moduleAlias.length() == 0) continue;
      //noinspection StringConcatenationInLoop
      autoimportedModules += String.format("import %s as %s\n", moduleName, moduleAlias);
    }
  }

  private JepInThread getCustomizedJepInstance(final Map<String, String> configuration) throws JepException {
    final JepInThread jep = new JepInThread();

    // add Jedi
    jep.eval("import jedi");

    // add custom python modules to sys_path (for Jedi)
    final String includePaths = configuration.get("python.jep.config.include.paths");
    if (StringUtils.isEmpty(includePaths)) {
      jep.eval("INCLUDE_PATHS_JEDI_ENV = None");
    } else {
      jep.eval("INCLUDE_PATHS_JEDI_ENV = jedi.get_default_environment()");
      for (final String path : includePaths.split(":")) {
        jep.eval(String.format("INCLUDE_PATHS_JEDI_ENV.get_sys_path().append('%s')", path));
      }
      jep.eval("INCLUDE_PATHS_JEDI_ENV = INCLUDE_PATHS_JEDI_ENV.get_sys_path()");
    }

    //this functions generate java-friendly list
    jep.eval("def parse_completions(completions):\n" +
        "    comp_list = []\n" +
        "    for comp in completions:\n" +
        "        comp_dict = {\n" +
        "            'name':        comp.name,\n" +
        "            'docString':   comp.docstring(),\n" +
        "            'description': comp.description,\n" +
        "            'type':        comp.type,\n" +
        "            'fullName':    comp.full_name\n" +
        "        }\n" +
        "        comp_list.append(comp_dict)\n" +
        "    return comp_list");

    return jep;
  }

  private static void loadJepLibrary(final Map<String, String> configuration) throws IOException {
    final String jepLibPath = configuration.get("python.jep.library.file");
    final String workDirPath = configuration.get("python.working.dir");
    final String newJepLibPath = workDirPath + "/" + System.mapLibraryName("jep");

    // copy Jep library
    Files.copy(Paths.get(jepLibPath), Paths.get(newJepLibPath), StandardCopyOption.REPLACE_EXISTING);
    addDir(Paths.get(newJepLibPath).getParent().toString());

    // load Jep library
    System.loadLibrary("jep");

    // delete Jep library after exit
    addDeleteFileHook(newJepLibPath);
  }

  private static void addDeleteFileHook(final String path) {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        Files.delete(Paths.get(path));
      } catch (final IOException e) {
        // ignore
      }
    }));
  }

  @Override
  public boolean isOpened() {
    return !Objects.isNull(jep);
  }

  @Override
  public boolean isAlive() {
    return !Objects.isNull(jep) && !jep.interrupted();
  }

  @Override
  public boolean isReusableForConfiguration(final Map<String, String> configuration) {
    return true;
  }

  @Override
  public void cancel() {

  }

  @Override
  public void close() {
    jep.close();
  }


  private static void addDir(String s) throws IOException {
    try {
      final  Field field = ClassLoader.class.getDeclaredField("usr_paths");
      field.setAccessible(true);
      final String[] paths = (String[])field.get(null);
      for (final String path : paths) {
        if (s.equals(path)) {
          return;
        }
      }
      final String[] tmp = new String[paths.length+1];
      System.arraycopy(paths,0,tmp,0,paths.length);
      tmp[paths.length] = s;
      field.set(null,tmp);
      System.setProperty("java.library.path", System.getProperty("java.library.path") + File.pathSeparator + s);
    } catch (final Exception e) {
      throw new IOException("Failed to add library path");
    }
  }
}

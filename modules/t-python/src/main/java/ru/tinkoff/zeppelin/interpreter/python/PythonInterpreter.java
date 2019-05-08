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
package ru.tinkoff.zeppelin.interpreter.python;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import ru.tinkoff.zeppelin.interpreter.Interpreter;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;

public class PythonInterpreter extends Interpreter {

  private String classPath;
  private ExecuteWatchdog watchdog;

  public PythonInterpreter() {
    super();
  }

  @Override
  public boolean isAlive() {
    return true;
  }

  @Override
  public boolean isOpened() {
    return this.classPath != null;
  }

  @Override
  public void open(Map<String, String> configuration, String classPath) {
    this.classPath = classPath;
  }

  @Override
  public boolean isReusableForConfiguration(Map<String, String> configuration) {
    return true;
  }

  @Override
  public void cancel() {
    if (watchdog != null) {
      watchdog.destroyProcess();
    }
  }

  @Override
  public void close() {
    if (watchdog != null) {
      watchdog.destroyProcess();
    }
  }

  @Override
  public InterpreterResult interpretV2(String st,
                                       Map<String, String> noteContext,
                                       Map<String, String> userContext,
                                       Map<String, String> configuration) {
    final Map<String, String> params = new HashMap<>();
    params.putAll(noteContext);
    params.putAll(userContext);

    final String pythonWorkingDir = configuration.get("python.working.dir");
    final String instanceTempFolder = pythonWorkingDir + "/" + getSessionUUID();
    final File instanceTempDir = new File(instanceTempFolder);
    params.put("Z_ENV_WORK_DIR", pythonWorkingDir);
    params.put("Z_ENV_TEMP_DIR", instanceTempFolder);

    final String jepLibraryFile = configuration.get("python.jep.library.file");
    final File jepLibrary = new File(jepLibraryFile);

    final long watchdogTime = Long.parseLong(configuration.get("python.watchdog.time"));
    watchdog = new ExecuteWatchdog(watchdogTime);

    final String additionalJvmArgs = configuration
        .getOrDefault("python.subprocess.java.args", StringUtils.EMPTY);

    try {
      if (!instanceTempDir.mkdirs()) {
        final InterpreterResult.Message errMessage = new InterpreterResult.Message(
                InterpreterResult.Message.Type.TEXT,
                "Error while create working directory"
        );
        final InterpreterResult result = new InterpreterResult(InterpreterResult.Code.ERROR);
        result.add(errMessage);
        return result;
      }

      if (!jepLibrary.exists()) {
        final InterpreterResult.Message errMessage = new InterpreterResult.Message(
                InterpreterResult.Message.Type.TEXT,
                String.format("Jep library does not exist at %s", jepLibrary.getAbsolutePath())
        );
        final InterpreterResult result = new InterpreterResult(InterpreterResult.Code.ERROR);
        result.add(errMessage);
        return result;
      }

      // copy jep lib into workingDir
      final String jepDest = instanceTempDir.getAbsolutePath() + "/" + "libjep.dylib";
      final File jepDestFile = new File(jepDest);
      FileUtils.copyFile(jepLibrary, jepDestFile);

      // write script
      final String scriptDest = instanceTempDir.getAbsolutePath() + "/" + getSessionUUID() + ".py";
      final File scriptFile = new File(scriptDest);
      FileUtils.write(new File(scriptDest), st, "UTF-8");

      // write params
      final Map<String, String> overridedParams = new HashMap<>();
      getAllEnvVariables(st).forEach(s -> overridedParams.put(s, "ZEPPELIN_NULL"));
      overridedParams.putAll(params);

      final String paramsDest = instanceTempDir.getAbsolutePath() + "/" + getSessionUUID() + ".params";
      final File paramsFile = new File(paramsDest);
      final Properties properties = new Properties();
      for (Map.Entry<String, String> entry : overridedParams.entrySet()) {
        properties.put(entry.getKey(), entry.getValue());
      }
      properties.store(new FileOutputStream(paramsFile), null);

      // create out
      final String outputDest = instanceTempDir.getAbsolutePath() + "/" + getSessionUUID() + ".out";
      final File outputFile = new File(outputDest);

      // run java
      final String cmd = String.format("java " +
                      "%s" +
                      " -cp \"%s/*\"" +
                      " -Djava.library.path=\"%s\"" +
                      " ru.tinkoff.zeppelin.interpreter.python.PythonInterpreterProcess" +
                      " -py_script \"%s\"" +
                      " -output_file \"%s\"" +
                      " -params_file \"%s\"" +
                      " -jep_include_paths \"%s\"" +
                      " -jep_python_home \"%s\"" +
                      " -storage_dir \"%s\"",
              additionalJvmArgs,
              classPath,
              jepDestFile.getParentFile().getAbsolutePath(),
              scriptFile.getAbsolutePath(),
              outputFile.getAbsolutePath(),
              paramsFile.getAbsolutePath(),
              configuration.getOrDefault("python.jep.config.include.paths", StringUtils.EMPTY),
              configuration.getOrDefault("python.jep.config.python.home", StringUtils.EMPTY),
              noteContext.get("Z_ENV_NOTE_STORAGE_DIR")
      );

      // start server process
      final DefaultExecutor executor = new DefaultExecutor();
      executor.setWorkingDirectory(instanceTempDir.getAbsoluteFile());
      executor.setWatchdog(watchdog);

      final ConcurrentLinkedQueue<InterpreterResult> interpreterResults = new ConcurrentLinkedQueue<>();

      final ExecuteResultHandler handler = new ExecuteResultHandler() {
        @Override
        public void onProcessComplete(final int exitValue) {
          interpreterResults.add(new InterpreterResult(InterpreterResult.Code.SUCCESS));
        }

        @Override
        public void onProcessFailed(final ExecuteException e) {
          interpreterResults.add(new InterpreterResult(InterpreterResult.Code.ERROR));
        }
      };

      final Map<String, String> env = new HashMap<>();
      /* for future */
      //env.put("PYTHONPATH", "");
      //env.put("PYTHONHOME", "");

      try {
        executor.execute(CommandLine.parse(cmd), env, handler);

      } catch (final Exception e) {
        final InterpreterResult.Message errMessage = new InterpreterResult.Message(
                InterpreterResult.Message.Type.TEXT,
                "Error while starting python process"
        );
        final InterpreterResult result = new InterpreterResult(InterpreterResult.Code.ERROR);
        result.add(errMessage);
        return result;
      }

      while (interpreterResults.isEmpty()) {
        try {
          Thread.sleep(50);
        } catch (final Exception e) {
          // SKIP
        }
      }

      final InterpreterResult result = interpreterResults.poll();
      // read files from dir and add interpreter result
      final File[] files = instanceTempDir.listFiles();
      if (files != null) {
        for (final File file : files) {
          final InterpreterResult.Message.Type type;
          final String extension = FilenameUtils.getExtension(file.getName()).toLowerCase();
          switch (extension) {
            case "html":
              type = InterpreterResult.Message.Type.HTML;
              break;
            case "img":
            case "jpeg":
            case "jpg":
            case "png":
              type = InterpreterResult.Message.Type.IMG;
              break;
            case "table":
              type = InterpreterResult.Message.Type.TABLE;
              break;
            case "txt":
            case "out":
            //case "params": // DEBUG
              type = InterpreterResult.Message.Type.TEXT;
              break;
            default:
              type = null;
          }
          if (type == InterpreterResult.Message.Type.IMG) {
            String payload = String.format(
                    "<div style='width:auto;height:auto'>" +
                            "<img src=data:image/%s;base64,%s  style='width=auto;height:auto'/>" +
                     "</div>",
                    extension,
                    new String(Base64.getEncoder().encode(FileUtils.readFileToByteArray(file))));
            result.add(new InterpreterResult.Message(InterpreterResult.Message.Type.HTML, payload));
          } else if (type != null) {
            result.add(new InterpreterResult.Message(type, FileUtils.readFileToString(file, "UTF-8")));
          }
        }
      }
      return result;

    } catch (final Exception e) {
      final InterpreterResult result = new InterpreterResult(InterpreterResult.Code.ERROR);
      result.add(new InterpreterResult.Message(InterpreterResult.Message.Type.TEXT, e.getLocalizedMessage()));
      return result;

    } finally {
      try {
        FileUtils.forceDelete(instanceTempDir);
      } catch (final Exception e) {
        // SKIP
      }
    }
  }
}

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

import jep.*;
import org.apache.commons.cli.*;
import sun.misc.Signal;

import java.io.*;
import java.nio.file.Files;
import java.util.*;

public class PythonInterpreterProcess {

  @SuppressWarnings("unchecked")
  public static void main(final String[] args) throws JepException {

    final Options options = new Options();
    final Option pyScriptPath = new Option("py_script",
            "py_script",
            true,
            "path to python script"
    );
    pyScriptPath.setRequired(true);
    options.addOption(pyScriptPath);

    final Option outputFilePath = new Option("output_file",
            "output_file",
            true,
            "path to python output"
    );
    outputFilePath.setRequired(true);
    options.addOption(outputFilePath);

    final Option paramsFilePath = new Option("params_file",
            "params_file",
            true,
            "path to python params file"
    );
    paramsFilePath.setRequired(true);
    options.addOption(paramsFilePath);

    final Option jepInclude = new Option("jep_include_paths",
            "jep_include_paths",
            true,
            "Sets a path of directories separated by File.pathSeparator that will be appended to the sub-intepreter's sys.path"
    );
    jepInclude.setRequired(false);
    options.addOption(jepInclude);

    final Option pythonHomePath = new Option("jep_python_home",
            "jep_python_home",
            true,
            "Python home directory path"
    );
    pythonHomePath.setRequired(false);
    options.addOption(pythonHomePath);

    final Option noteStorageDir = new Option("storage_dir",
            "storage_dir",
            true,
            "Note storage dir"
    );
    noteStorageDir.setRequired(false);
    options.addOption(noteStorageDir);

    final Option autoimpoerModules = new Option("auto_import",
            "auto_import",
            true,
            "auto import modules"
    );
    autoimpoerModules.setRequired(false);
    options.addOption(autoimpoerModules);

    CommandLine cmd = null;
    try {
      cmd = new DefaultParser().parse(options, args);
    } catch (ParseException e) {
      new HelpFormatter().printHelp("utility-name", options);
      System.exit(1);
    }

    final String pathToScript = cmd.getOptionValue("py_script");
    final String pathToOutput = cmd.getOptionValue("output_file");
    final String pathToParamsFile = cmd.getOptionValue("params_file");
    final String jepIncludePaths = cmd.getOptionValue("jep_include_paths");
    final String jepPythonHome = cmd.getOptionValue("jep_python_home");
    final String noteStorage = cmd.getOptionValue("storage_dir");
    final String autoimport = cmd.getOptionValue("auto_import");

    final JepConfig jepConfig = new JepConfig()
            .setRedirectOutputStreams(true)
            .addIncludePaths(jepIncludePaths.split(":"));

    if (!"".equals(jepPythonHome)) {
      PyConfig pyConfig = new PyConfig();
      pyConfig.setPythonHome(jepPythonHome);
      MainInterpreter.setInitParams(pyConfig);
    }

    final File output = new File(pathToOutput);
    try (final FileOutputStream fos = new FileOutputStream(output, true);
         final BufferedOutputStream bos = new BufferedOutputStream(fos);
         final PrintStream ps = new PrintStream(bos)) {

      System.setOut(ps);
      System.setErr(ps);
      Signal.handle(new Signal("TERM"), signal -> {
        ps.println("Process terminated by external signal / watchdog timeout");
        ps.close();
        System.exit(1);
      });

      // Lambda Runnable
      final Runnable flusher = () -> {
        while (!Thread.interrupted()) {
          try {
            ps.flush();
            bos.flush();
            fos.flush();
            Thread.sleep(10);
          } catch (final Exception e) {
            //Skip
          }
        }
      };
      final Thread flusherThread = new Thread(flusher);
      flusherThread.start();

      try (final Jep jep = new Jep(jepConfig)) {
        jep.setInteractive(true);

        // inject values into python
        // load runtime properties from file
        Properties properties = new Properties();
        properties.load(new FileInputStream(pathToParamsFile));

        // inject values into python
        for (String key : properties.stringPropertyNames()) {
          if (properties.get(key).toString().equals("ZEPPELIN_NULL")) {
            continue;
          }
          jep.set(key, properties.get(key).toString());
        }
        jep.eval("import pickle as zpickle");
        jep.eval("import dill as zdill");

        // inject autiomported modules
        final List<String> modules = autoimport.length() > 1
                ? Arrays.asList(autoimport.split(";"))
                : new ArrayList<>();
        for (final String module : modules) {
          final String[] data = module.split(":");
          if (data.length != 2) {
            continue;
          }
          jep.eval(String.format("import %s as %s", data[0], data[1]));
        }

        // load current environment
        final Set<String> envBeforeEval = new HashSet<>((List<String>)jep.getValue("list(locals().keys())"));


        Files.createDirectories(new File(noteStorage).toPath());
        final Map<String, PythonInterpreterEnvObject> envObjects = new HashMap<>();
        final File noteContextFile = new File(noteStorage + "/note.context");
        if (noteContextFile.exists()) {
          FileInputStream fis = new FileInputStream(noteContextFile);
          ObjectInputStream ois = new ObjectInputStream(fis);

          envObjects.putAll((Map<String, PythonInterpreterEnvObject>) ois.readObject());

          for (final PythonInterpreterEnvObject envObject : envObjects.values()) {
            switch (envObject.getClassName()) {
              case "FUNC":
                jep.set(envObject.getName() + "_ZZ", new NDArray<>(envObject.getPayload()));
                jep.eval(envObject.getName() + " =  zdill.loads(" + envObject.getName() + "_ZZ)");
                jep.eval("del " + envObject.getName() + "_ZZ");
                break;
              case "MODULE":
                final String[] pair = new String(envObject.getPayload()).split(":");
                jep.eval(String.format("import %s as %s", pair[0], pair[1]));
                break;
              default:
                jep.set(envObject.getName() + "_ZZ", new NDArray<>(envObject.getPayload()));
                jep.eval(envObject.getName() + " =  zpickle.loads(" + envObject.getName() + "_ZZ)");
                jep.eval("del " + envObject.getName() + "_ZZ");
            }
          }
        }


        // execute script
        jep.runScript(pathToScript);
        flusherThread.interrupt();

        final Set<String> envAfterEval = new HashSet<>((List<String>)jep.getValue("list(locals().keys())"));
        envAfterEval.removeAll(envBeforeEval);

        for (final String env : envAfterEval) {
          final String type = jep.getValue("type(" + env + ")").toString();
          switch (type) {
            case "<class 'function'>":
              final PythonInterpreterEnvObject pieoF = new PythonInterpreterEnvObject(
                      env,
                      "FUNC",
                      jep.getValue_bytearray("zdill.dumps(" + env + ")")
              );
              envObjects.put(pieoF.getName(), pieoF);
              break;
            case "<class 'module'>":
              final String val = ((String) jep.getValue(env));
              final int startIndex = val.indexOf("'") + 1;
              final int endIndex = val.indexOf("'", startIndex);
              final String module = val.substring(startIndex, endIndex);

              final String payload = module + ":" + env;
              final PythonInterpreterEnvObject pieoM = new PythonInterpreterEnvObject(
                      env,
                      "MODULE",
                      payload.getBytes()
              );
              envObjects.put(pieoM.getName(), pieoM);
              break;
            default:
              final PythonInterpreterEnvObject pieoD = new PythonInterpreterEnvObject(
                      env,
                      "OBJECT",
                      jep.getValue_bytearray("zpickle.dumps(" + env + ", protocol=0)")
              );
              envObjects.put(pieoD.getName(), pieoD);
          }
        }
        FileOutputStream fout = new FileOutputStream(noteContextFile);
        ObjectOutputStream oos = new ObjectOutputStream(fout);
        oos.writeObject(envObjects);

        // sleep for 100 ms
        Thread.sleep(100);

      } catch (final JepException je) {
        ps.println(je.getLocalizedMessage());
        throw new RuntimeException("Error");

      } catch (final Throwable e) {
        e.printStackTrace(ps);
        throw new RuntimeException("Error");
      }

    } catch (final Throwable e) {
      System.exit(1);
    }
    System.exit(0);
  }
}

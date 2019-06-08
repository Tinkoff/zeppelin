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

import ru.tinkoff.zeppelin.interpreter.python.PythonInterpreterEnvObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class NoteContextLoader {
  private final File contextDir;
  private final ConcurrentHashMap<String, NoteContext> contextMap;
  private final ConcurrentHashMap<String, Long> timeOfLastUse;
  private final Long CACHE_TTL = 600_000L; // 10 minute

  NoteContextLoader(final String contextDirPath) {
    contextDir = new File(contextDirPath);
    contextMap = new ConcurrentHashMap<>();
    timeOfLastUse = new ConcurrentHashMap<>();
    createClearCacheThread();
  }

  String getInitScript(final String noteUUID) {
    contextMap.putIfAbsent(noteUUID, new NoteContext(noteUUID));
    final NoteContext context = contextMap.get(noteUUID);

    if (context.lastModifiedDate != context.file.lastModified()) {
      context.updateContextScript();
    }
    timeOfLastUse.put(noteUUID, System.currentTimeMillis());
    return context.contextInitScript;
  }

  private class NoteContext {

    private String contextInitScript;
    Long lastModifiedDate;
    final File file;

    NoteContext(final String noteUUID) {
      file = new File(contextDir.getAbsoluteFile() + "/" + noteUUID + "/note.context");
      lastModifiedDate = 0L;
      contextInitScript = "";
    }

    private void updateContextScript() {
      if (!file.exists()) {
        return;
      }

      try (final FileInputStream fis = new FileInputStream(file);
           final ObjectInputStream ois = new ObjectInputStream(fis)) {
        final StringBuilder script = new StringBuilder();
        //noinspection unchecked
        final Map<String, PythonInterpreterEnvObject> envObjects =
            new HashMap<>((Map<String, PythonInterpreterEnvObject>) ois.readObject());

        for (final PythonInterpreterEnvObject envObject : envObjects.values()) {
          switch (envObject.getClassName()) {
            case "FUNC":
              // describe simple function
              script.append("def ").append(envObject.getName()).append("():\n").append("\treturn None\n");
              break;
            case "MODULE":
              // add module
              final String[] pair = new String(envObject.getPayload()).split(":");
              script.append(String.format("import %s as %s\n", pair[0], pair[1]));
              break;
            default:
              // add expression
              script.append(envObject.getName()).append(" = NO_DATA\n");
          }
        }
        contextInitScript = script.toString();
      } catch (final Exception e) {
        contextInitScript = "";
      } finally {
        lastModifiedDate = file.lastModified();
      }
    }
  }

  private void createClearCacheThread() {
    final Thread clearThread = new Thread() {
      @Override
      public void run() {
        while (!isInterrupted()) {
          try {
            final Iterator<Map.Entry<String, Long>> iterator = timeOfLastUse.entrySet().iterator();
            while (iterator.hasNext()) {
              final Map.Entry<String, Long> entry = iterator.next();
              if (System.currentTimeMillis() - entry.getValue() > CACHE_TTL) {
                iterator.remove();
                contextMap.remove(entry.getKey());
              }
            }
            Thread.sleep(60_000); // 1 minute
          } catch (final Exception e) {
            // ignore
          }
        }
      }
    };
    clearThread.setDaemon(true);
    clearThread.setName("clear-thread");
    clearThread.start();
  }
}

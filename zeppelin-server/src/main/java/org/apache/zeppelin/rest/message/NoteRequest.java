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
package org.apache.zeppelin.rest.message;

import com.google.gson.Gson;
import java.util.Map;
import java.util.Set;

import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.NoteRevision;

@SuppressWarnings({"FieldCanBeLocal", "unused"})
public class NoteRequest {

  private static final Gson gson = new Gson();

  // use for updated note
  private String path;
  private Set<String> owners;
  private Set<String> readers;
  private Set<String> runners;
  private Set<String> writers;

  // use only for send note back to user
  private Long id;
  private String uuid;
  private NoteRevision revision;
  private Map<String, Object> formParams;
  private boolean isRunning;

  public NoteRequest(final Note note) {
    path = note.getPath();
    owners = note.getOwners();
    readers = note.getReaders();
    runners = note.getRunners();
    writers = note.getWriters();
    id = note.getId();
    uuid = note.getUuid();
    revision = note.getRevision();
    formParams = note.getFormParams();
  }

  public NoteRequest() {
  }

  public Note getAsNote() {
    final Note note = new Note(path);
    note.getOwners().addAll(owners);
    note.getReaders().addAll(readers);
    note.getRunners().addAll(runners);
    note.getWriters().addAll(writers);
    return note;
  }

  public String getPath() {
    return path;
  }

  public String toJson() {
    return gson.toJson(this);
  }

  public Set<String> getOwners() {
    return owners;
  }

  public Set<String> getReaders() {
    return readers;
  }

  public Set<String> getRunners() {
    return runners;
  }

  public Set<String> getWriters() {
    return writers;
  }

  public static NoteRequest fromJson(final String json) {
    final NoteRequest noteRequest = gson.fromJson(json, NoteRequest.class);
    if (noteRequest.path != null) {
      noteRequest.path = normalizePath(noteRequest.path);
    }
    return noteRequest;
  }

  private static String normalizePath(String path) {
    // fix 'folder/noteName' --> '/folder/noteName'
    if (!path.startsWith("/")) {
      path = "/" + path;
    }

    // fix '///folder//noteName' --> '/folder/noteName'
    while (path.contains("//")) {
      path = path.replaceAll("//", "/");
    }

    //fix '/folder/noteName/' --> '/folder/noteName'
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    return path;
  }
}

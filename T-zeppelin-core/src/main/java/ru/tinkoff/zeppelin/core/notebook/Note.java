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

package ru.tinkoff.zeppelin.core.notebook;

import java.io.File;
import java.io.Serializable;
import java.util.*;

import org.apache.zeppelin.utils.IdHashes;

/**
 * Represent the note of Zeppelin. All the note and its paragraph operations are done
 * via this class.
 */
public class Note implements Serializable {

  /**
   * Note representing mode.
   */
  public enum NoteViewMode {
    SIMPLE,
    DEFAULT,
    REPORT,
    SCHEMA
  }

  public final static String TRASH_FOLDER = "~Trash";

  private Long id;
  private Long batchJobId;
  private String uuid;
  private String name;
  private String path;
  private NoteRevision revision;
  private NoteViewMode viewMode;

  private Map<String, Object> formParams;

  /********************************** user permissions info *************************************/
  private final Set<String> owners;
  private final Set<String> readers;
  private final Set<String> runners;
  private final Set<String> writers;

  public Note(final String path) {
    this(null, path);
    this.name = path.substring(path.lastIndexOf(File.separator) + 1);
  }

  public Note(final String name, final String path) {
    this.uuid = IdHashes.generateId();
    this.name = name;
    this.path = path;
    this.viewMode = NoteViewMode.DEFAULT;

    this.formParams = new HashMap<>();

    this.owners = new HashSet<>();
    this.readers = new HashSet<>();
    this.runners = new HashSet<>();
    this.writers = new HashSet<>();
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(final String uuid) {
    this.uuid = uuid;
  }

  public Long getId() {
    return id;
  }

  public void setId(final long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public Map<String, Object> getFormParams() {
    return formParams;
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

  public String getPath() {
    return path;
  }

  public void setPath(final String path) {
    this.path = path;
    this.name = path.substring(path.lastIndexOf(File.separator) + 1);
  }

  public NoteRevision getRevision() {
    return revision;
  }

  public void setRevision(final NoteRevision revision) {
    this.revision = revision;
  }

  public boolean isTrashed() {
    return this.path.startsWith("/" + TRASH_FOLDER);
  }

  public Long getBatchJobId() {
    return batchJobId;
  }

  public void setBatchJobId(final Long batchJobId) {
    this.batchJobId = batchJobId;
  }

  public NoteViewMode getViewMode() {
    return viewMode;
  }

  public void setViewMode(final NoteViewMode viewMode) {
    this.viewMode = viewMode;
  }

  @Override
  public String toString() {
    if (this.path != null) {
      return this.path;
    } else {
      return "/" + this.name;
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof Note)) return false;
    final Note note = (Note) o;
    return Objects.equals(id, note.id) &&
        Objects.equals(batchJobId, note.batchJobId) &&
        Objects.equals(uuid, note.uuid) &&
        Objects.equals(name, note.name) &&
        Objects.equals(path, note.path) &&
        Objects.equals(revision, note.revision) &&
        Objects.equals(formParams, note.formParams) &&
        Objects.equals(owners, note.owners) &&
        Objects.equals(readers, note.readers) &&
        Objects.equals(runners, note.runners) &&
        Objects.equals(writers, note.writers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, batchJobId, uuid, name, path, revision, formParams, owners, readers, runners, writers);
  }
}

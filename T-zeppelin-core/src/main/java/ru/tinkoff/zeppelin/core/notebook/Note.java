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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.zeppelin.utils.IdHashes;

/**
 * Represent the note of Zeppelin. All the note and its paragraph operations are done
 * via this class.
 */
public class Note implements Serializable {

  public final static String TRASH_FOLDER = "~Trash";

  private Long id;
  private Long batchJobId;
  private String uuid;
  private String name;
  private String path;
  private NoteRevision revision;

  private Map<String, Object> formParams;

  private Scheduler scheduler;

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

    this.formParams = new HashMap<>();

    this.owners = new HashSet<>();
    this.readers = new HashSet<>();
    this.runners = new HashSet<>();
    this.writers = new HashSet<>();

    this.scheduler = null;
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

  public void setScheduler(final Scheduler scheduler) {
    this.scheduler = scheduler;
  }

  public Scheduler getScheduler() {
    return scheduler;
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

  @Override
  public String toString() {
    if (this.path != null) {
      return this.path;
    } else {
      return "/" + this.name;
    }
  }
}

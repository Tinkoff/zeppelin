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

import java.io.Serializable;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Interpreter source. Installs at runtime by maven artifact.
 */
public class ModuleSource implements Serializable {

  /**
   * Types of interpreter processes.
   */
  public enum Status {
    NOT_INSTALLED,
    INSTALLED
  }

  public enum Type {
    INTERPRETER,
    COMPLETER
  }

  private long id;

  private String name;

  private Type type;

  private String artifact;

  private Status status;

  /**
   * {@code null} if source is not installed, absolute path otherwise.
   */
  private String path;

  /**
   * if {@code true} - source would be reinstalled on app restart.
   */
  private boolean reinstallOnStart;


  public ModuleSource(final long id,
                      final String name,
                      final Type type,
                      final String artifact,
                      final Status status,
                      final String path,
                      final boolean reinstallOnStart) {
    this.id = id;
    this.name = name;
    this.type = type;
    this.artifact = artifact;
    this.status = status;
    this.path = path;
    this.reinstallOnStart = reinstallOnStart;
  }

  public long getId() {
    return id;
  }

  public void setId(final long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public Type getType() {
    return type;
  }

  public void setType(final Type type) {
    this.type = type;
  }

  public String getArtifact() {
    return artifact;
  }

  public void setArtifact(final String artifact) {
    this.artifact = artifact;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(final Status status) {
    this.status = status;
  }

  public String getPath() {
    return path;
  }

  public void setPath(final String path) {
    this.path = path;
  }

  public boolean isReinstallOnStart() {
    return reinstallOnStart;
  }

  public void setReinstallOnStart(final boolean reinstallOnStart) {
    this.reinstallOnStart = reinstallOnStart;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("name='" + name + "'")
        .add("artifact='" + artifact + "'")
        .add("path='" + path + "'")
        .add("status='" + status + "'")
        .add("reinstallOnStart='" + reinstallOnStart + "'")
        .toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof ModuleSource)) return false;
    final ModuleSource source = (ModuleSource) o;
    return id == source.id &&
        reinstallOnStart == source.reinstallOnStart &&
        Objects.equals(name, source.name) &&
        type == source.type &&
        Objects.equals(artifact, source.artifact) &&
        status == source.status &&
        Objects.equals(path, source.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, type, artifact, status, path, reinstallOnStart);
  }
}

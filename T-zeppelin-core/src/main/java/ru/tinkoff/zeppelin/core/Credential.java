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

package ru.tinkoff.zeppelin.core;

import java.io.Serializable;
import java.util.*;

/**
 * Credential is user-information, available in interpolation.
 */
public class Credential implements Serializable {

  private long id;

  private final String key;

  private String value;

  private Boolean secure;

  private String description;

  /**
   * Users who could delete/edit.
   */
  private final Set<String> owners;

  /**
   * Users who could use it in context.
   */
  private final Set<String> readers;

  public Credential(final long id,
                    final String key,
                    final String value,
                    final boolean secure,
                    final String description,
                    final Set<String> readers,
                    final Set<String> owners) {
    this.id = id;
    this.key = key;
    this.value = value;
    this.secure = secure;
    this.description = description;
    this.readers = new HashSet<>(readers);
    this.owners = new HashSet<>(owners);
  }


  public long getId() {
    return id;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public Boolean getSecure() {
    return secure;
  }

  public String getDescription() {
    return description;
  }

  public Set<String> getOwners() {
    return Collections.unmodifiableSet(new HashSet<>(owners));
  }

  public Set<String> getReaders() {
    return Collections.unmodifiableSet(new HashSet<>(readers));
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
            .add("id='" + id + "'")
            .add("key='" + key + "'")
            .add("value='" + value + "'")
            .add("description='" + description + "'")
            .add("owners=" + owners)
            .add("readers=" + readers)
            .toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Credential)) {
      return false;
    }
    final Credential that = (Credential) o;
    return id == that.id &&
            key.equals(that.key) &&
            value.equals(that.value) &&
            Objects.equals(description, that.description) &&
            owners.equals(that.owners) &&
            readers.equals(that.readers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, key, value, description, owners, readers);
  }
}

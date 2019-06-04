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

package ru.tinkoff.zeppelin.interpreter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

public class InterpreterCompletion implements Comparable<InterpreterCompletion> {

  /**
   * Name to show in completion list.
   */
  private final String name;

  /**
   * Value which would be inserted.
   */
  private final String value;

  /**
   * Type of element on completion list, e.g. schema, table, keyword.
   */
  private final String meta;

  /**
   * Description of element.
   */
  private final String description;

  /**
   * Priority of an element in completion list.
   */
  private int score;

  public InterpreterCompletion(final String name,
                               final String value,
                               final String meta,
                               final String description,
                               final int score) {
    this(name, value, meta, description);
    this.score = score;
  }

  public InterpreterCompletion(final String name,
                               final String value,
                               final String meta,
                               final String description) {
    this.name = name;
    this.value = value;
    this.meta = meta;
    this.description = description;

    final Set<String> preferredKeywords = new HashSet<>(
        Arrays.asList("select", "*", "join", "from", "update", "insert", "delete", "with", "set", "on"));

    if (meta.equals("column")) {
      this.score = 900;
    } else if (meta.equals("table")) {
      this.score = 700;
    } else if (meta.equals("schema")) {
      this.score = 500;
    } else if (preferredKeywords.contains(value)) {
      this.score = 400;
    } else {
      this.score = 300;
    }
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("name='" + name + "'")
        .add("value='" + value + "'")
        .add("meta='" + meta + "'")
        .add("description='" + description + "'")
        .add("score=" + score)
        .toString();
  }

  @Override
  public int compareTo(final InterpreterCompletion interpreterCompletion) {
    return name.compareToIgnoreCase(interpreterCompletion.getName());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InterpreterCompletion)) {
      return false;
    }
    final InterpreterCompletion that = (InterpreterCompletion) o;
    return score == that.score &&
        Objects.equals(name, that.name) &&
        Objects.equals(value, that.value) &&
        Objects.equals(meta, that.meta) &&
        Objects.equals(description, that.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value, meta, description, score);
  }
}
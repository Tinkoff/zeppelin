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

package ru.tinkoff.zeppelin.core.content;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.Map;
import java.util.StringJoiner;

/**
 * Represents user content.
 */
public class Content {

  private Long id;

  private Long noteId;

  private ContentType type;

  private String description;

  private String location;

  private final Map<String, JsonElement> params;

  public static TableContent getAsTableContent(final Content content) {
    return new TableContent(content);
  }

  public Content(final Long noteId,
                 final ContentType type,
                 final String description,
                 final String location,
                 final Map<String, JsonElement> params) {
    this.noteId = noteId;
    this.type = type;
    this.description = description;
    this.location = location;
    this.params = params;
  }

  public Content(final Long id,
                 final Long noteId,
                 final ContentType type,
                 final String description,
                 final String location,
                 final Map<String, JsonElement> params) {
    this.noteId = noteId;
    this.type = type;
    this.description = description;
    this.location = location;
    this.id = id;
    this.params = params;
  }


  public Long getId() {
    return id;
  }

  public void setId(final Long id) {
    this.id = id;
  }


  public Long getNoteId() {
    return noteId;
  }

  public void setNoteId(final Long noteId) {
    this.noteId = noteId;
  }


  public ContentType getType() {
    return type;
  }

  public void setType(final ContentType type) {
    this.type = type;
  }


  public String getDescription() {
    return description;
  }

  public void setDescription(final String description) {
    this.description = description;
  }


  public String getLocation() {
    return location;
  }

  public void setLocation(final String location) {
    this.location = location;
  }

  public Map<String, JsonElement> getParams() {
    return params;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
            .add("id=" + id)
            .add("noteId=" + noteId)
            .add("type=" + type)
            .add("description='" + description + "'")
            .add("location='" + location + "'")
            .toString();
  }
}

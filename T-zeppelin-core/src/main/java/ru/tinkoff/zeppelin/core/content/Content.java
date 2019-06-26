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

import java.util.StringJoiner;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents user content.
 */
public class Content {

  @Nonnull
  private Long databaseId;

  @Nonnull
  private Long noteId;

  @Nonnull
  private ContentType type;

  @Nullable
  private String description;

  @Nonnull
  private String location;

  public Content(@Nonnull final Long databaseId,
                 @Nonnull final Long noteId,
                 @Nonnull final ContentType type,
                 @Nullable final String description,
                 @Nonnull final String location) {
    this.databaseId = databaseId;
    this.noteId = noteId;
    this.type = type;
    this.description = description;
    this.location = location;
  }

  @Nonnull
  public Long getDatabaseId() {
    return databaseId;
  }

  public void setDatabaseId(@Nonnull final Long databaseId) {
    this.databaseId = databaseId;
  }

  @Nonnull
  public Long getNoteId() {
    return noteId;
  }

  public void setNoteId(@Nonnull final Long noteId) {
    this.noteId = noteId;
  }

  @Nonnull
  public ContentType getType() {
    return type;
  }

  public void setType(@Nonnull final ContentType type) {
    this.type = type;
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  public void setDescription(@Nullable final String description) {
    this.description = description;
  }

  @Nonnull
  public String getLocation() {
    return location;
  }

  public void setLocation(@Nonnull final String location) {
    this.location = location;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("databaseId=" + databaseId)
        .add("noteId=" + noteId)
        .add("type=" + type)
        .add("description='" + description + "'")
        .add("location='" + location + "'")
        .toString();
  }
}

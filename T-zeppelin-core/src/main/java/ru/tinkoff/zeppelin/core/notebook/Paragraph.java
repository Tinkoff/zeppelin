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

import java.io.Serializable;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Paragraph is a POJO which represents Note's sub-element.
 */
public class Paragraph implements Serializable {

  private Long id;
  private Long noteId;
  private final String uuid;
  private String title;
  private String text;
  private String shebang;
  private LocalDateTime created;
  private LocalDateTime updated;
  private Integer position;
  private Long jobId;
  private Long revisionId;

  private transient String selectedText;

  //paragraph configs like isOpen, colWidth, etc
  private Map<String, Object> config;

  // form and parameter settings
  private Map<String, Object> formParams;


  public Paragraph() {
    this.uuid = "paragraph_" + System.currentTimeMillis() + "_" + new SecureRandom().nextInt();
    config = new HashMap<>(5);
    formParams = new HashMap<>(1);
  }

  public Paragraph(final Long id,
                   final Long noteId,
                   final String uuid,
                   final String title,
                   final String text,
                   final String shebang,
                   final LocalDateTime created,
                   final LocalDateTime updated,
                   final Integer position,
                   final Long jobId,
                   final Long revisionId,
                   final Map<String, Object> config,
                   final Map<String, Object> formParams) {
    this.id = id;
    this.noteId = noteId;
    this.uuid = uuid;
    this.title = title;
    this.text = text;
    this.shebang = shebang;
    this.created = created;
    this.updated = updated;
    this.position = position;
    this.jobId = jobId;
    this.revisionId = revisionId;
    this.config = config;
    this.formParams = formParams;
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

  public String getUuid() {
    return uuid;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(final String title) {
    this.title = title;
  }

  public String getText() {
    return text;
  }

  public void setText(final String text) {
    this.text = text;
  }

  public String getSelectedText() {
    return selectedText;
  }

  public void setSelectedText(final String selectedText) {
    this.selectedText = selectedText;
  }

  public String getShebang() {
    return shebang;
  }

  public void setShebang(final String shebang) {
    this.shebang = shebang;
  }

  public LocalDateTime getCreated() {
    return created;
  }

  public void setCreated(final LocalDateTime created) {
    this.created = created;
  }

  public LocalDateTime getUpdated() {
    return updated;
  }

  public void setUpdated(final LocalDateTime updated) {
    this.updated = updated;
  }

  public Integer getPosition() {
    return position;
  }

  public void setPosition(final Integer position) {
    this.position = position;
  }

  public Long getJobId() {
    return jobId;
  }

  public void setJobId(final Long jobId) {
    this.jobId = jobId;
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  public Map<String, Object> getFormParams() {
    return formParams;
  }

  public Long getRevisionId() {
    return revisionId;
  }

  public void setRevisionId(final Long revisionId) {
    this.revisionId = revisionId;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof Paragraph)) return false;
    final Paragraph paragraph = (Paragraph) o;
    return Objects.equals(id, paragraph.id) &&
        Objects.equals(noteId, paragraph.noteId) &&
        Objects.equals(uuid, paragraph.uuid) &&
        Objects.equals(title, paragraph.title) &&
        Objects.equals(text, paragraph.text) &&
        Objects.equals(shebang, paragraph.shebang) &&
        Objects.equals(created, paragraph.created) &&
        Objects.equals(updated, paragraph.updated) &&
        Objects.equals(position, paragraph.position) &&
        Objects.equals(jobId, paragraph.jobId) &&
        Objects.equals(revisionId, paragraph.revisionId) &&
        Objects.equals(selectedText, paragraph.selectedText) &&
        Objects.equals(config, paragraph.config) &&
        Objects.equals(formParams, paragraph.formParams);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, noteId, uuid, title, text, shebang, created,
        updated, position, jobId, revisionId, selectedText, config, formParams);
  }
}
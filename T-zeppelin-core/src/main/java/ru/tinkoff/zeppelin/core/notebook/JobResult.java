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

import java.time.LocalDateTime;
import java.util.Objects;

public class JobResult {

  private Long id;
  private Long jobId;
  private LocalDateTime createdAt;
  private String type;
  private String result;

  public Long getId() {
    return id;
  }

  public void setId(final Long id) {
    this.id = id;
  }

  public Long getJobId() {
    return jobId;
  }

  public void setJobId(final Long jobId) {
    this.jobId = jobId;
  }

  public LocalDateTime getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(final LocalDateTime createdAt) {
    this.createdAt = createdAt;
  }

  public String getType() {
    return type;
  }

  public void setType(final String type) {
    this.type = type;
  }

  public String getResult() {
    return result;
  }

  public void setResult(final String result) {
    this.result = result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof JobResult)) return false;
    final JobResult jobResult = (JobResult) o;
    return Objects.equals(id, jobResult.id) &&
        Objects.equals(jobId, jobResult.jobId) &&
        Objects.equals(createdAt, jobResult.createdAt) &&
        Objects.equals(type, jobResult.type) &&
        Objects.equals(result, jobResult.result);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, jobId, createdAt, type, result);
  }
}

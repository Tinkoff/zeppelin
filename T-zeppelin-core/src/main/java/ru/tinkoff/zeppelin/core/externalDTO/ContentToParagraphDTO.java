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

package ru.tinkoff.zeppelin.core.externalDTO;

public class ContentToParagraphDTO {
  private long contentId;
  private long paragraphId;

  public ContentToParagraphDTO(final long contentId, final long paragraphId) {
    this.contentId = contentId;
    this.paragraphId = paragraphId;
  }

  public long getContentId() {
    return contentId;
  }

  public void setContentId(final long contentId) {
    this.contentId = contentId;
  }

  public long getParagraphId() {
    return paragraphId;
  }

  public void setParagraphId(final long paragraphId) {
    this.paragraphId = paragraphId;
  }
}

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

import java.util.HashMap;
import java.util.Map;

public class Context {

  public enum StartType{
    REGULAR,
    SCHEDULED,
    ;
  }

  private final Long noteId;
  private final String noteUUID;
  private final long pragraphId;
  private final String paragraphUUID;
  private final StartType startType;
  private final Map<String, String> noteContext = new HashMap<>();
  private final Map<String, String> userContext = new HashMap<>();
  private final Map<String, String> configuration = new HashMap<>();

  public Context(Long noteId, String noteUUID, long pragraphId, String paragraphUUID, StartType startType) {
    this.noteId = noteId;
    this.noteUUID = noteUUID;
    this.pragraphId = pragraphId;
    this.paragraphUUID = paragraphUUID;
    this.startType = startType;
  }

  public Long getNoteId() {
    return noteId;
  }

  public String getNoteUUID() {
    return noteUUID;
  }

  public long getPragraphId() {
    return pragraphId;
  }

  public String getParagraphUUID() {
    return paragraphUUID;
  }

  public StartType getStartType() {
    return startType;
  }

  public Map<String, String> getNoteContext() {
    return noteContext;
  }

  public Map<String, String> getUserContext() {
    return userContext;
  }

  public Map<String, String> getConfiguration() {
    return configuration;
  }
}

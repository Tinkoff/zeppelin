package ru.tinkoff.zeppelin.core.externalDTO;
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

import java.io.File;
import java.time.LocalDateTime;

public class InterpreterQueueDTO {

    private final Long id;
    private final Long noteId;
    private final String noteUuid;
    private final String noteName;
    private final Long paragraphId;
    private final String paragraphUuid;
    private final Long paragraphPosition;
    private final String shebang;
    private final String username;
    private final LocalDateTime startedAt;

    public InterpreterQueueDTO(final Long id,
                               final Long noteId,
                               final String noteUuid,
                               final String path,
                               final Long paragraphId,
                               final String paragraphUuid,
                               final Long paragraphPosition,
                               final String shebang,
                               final String username,
                               final LocalDateTime startedAt) {
        this.id = id;
        this.noteId = noteId;
        this.noteUuid = noteUuid;
        this.noteName = path.substring(path.lastIndexOf(File.separator) + 1);
        this.paragraphId = paragraphId;
        this.paragraphUuid = paragraphUuid;
        this.paragraphPosition = paragraphPosition;
        this.shebang = shebang;
        this.username = username;
        this.startedAt = startedAt;
    }

    public Long getId() {
        return id;
    }

    public Long getNoteId() {
        return noteId;
    }

    public Long getParagraphId() {
        return paragraphId;
    }

    public Long getParagraphPosition() {
        return paragraphPosition;
    }

    public String getShebang() {
        return shebang;
    }

    public String getUsername() {
        return username;
    }

    public String getNoteUuid() {
        return noteUuid;
    }
}

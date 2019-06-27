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
package org.apache.zeppelin.rest;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.core.content.Content;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.NoteContext;
import ru.tinkoff.zeppelin.storage.ContentDAO;

import java.io.File;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/h2")
public class H2RestApi {
  Logger LOGGER = LoggerFactory.getLogger(H2RestApi.class);
  private final Configuration configuration;
  private final ContentDAO contentDAO;

  @Autowired
  public H2RestApi(final Configuration configuration,
                   final ContentDAO contentDAO) {
    this.configuration = configuration;
    this.contentDAO = contentDAO;
  }

  @GetMapping(value = "/get_tables_list/{noteId}", produces = "application/json")
  public ResponseEntity getH2SavedTablesList(@PathVariable("noteId") final Long noteId) {
    return new JsonResponse(HttpStatus.OK,contentDAO.getContent(noteId)).build();
  }
}

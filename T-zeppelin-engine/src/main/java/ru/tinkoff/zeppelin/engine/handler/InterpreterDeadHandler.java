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
package ru.tinkoff.zeppelin.engine.handler;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.engine.NoteEventService;
import ru.tinkoff.zeppelin.storage.*;

import java.util.List;

/**
 * Class for handle dead interpreter process
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
@Component
public class InterpreterDeadHandler extends AbstractHandler {

  public InterpreterDeadHandler(final JobBatchDAO jobBatchDAO,
                                final JobDAO jobDAO,
                                final JobResultDAO jobResultDAO,
                                final JobPayloadDAO jobPayloadDAO,
                                final NoteDAO noteDAO,
                                final ParagraphDAO paragraphDAO,
                                final FullParagraphDAO fullParagraphDAO,
                                final NoteEventService noteEventService) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO, noteEventService);
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handle(final List<String> liveInterpreters) {
    liveInterpreters.add("__DEFAULT__");
    jobDAO.restoreState(liveInterpreters);
  }

}

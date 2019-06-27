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

import java.util.List;
import java.util.Set;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.SystemEvent;
import ru.tinkoff.zeppelin.core.notebook.*;
import ru.tinkoff.zeppelin.engine.NoteEventService;
import ru.tinkoff.zeppelin.storage.FullParagraphDAO;
import ru.tinkoff.zeppelin.storage.JobBatchDAO;
import ru.tinkoff.zeppelin.storage.JobDAO;
import ru.tinkoff.zeppelin.storage.JobPayloadDAO;
import ru.tinkoff.zeppelin.storage.JobResultDAO;
import ru.tinkoff.zeppelin.storage.NoteDAO;
import ru.tinkoff.zeppelin.storage.ParagraphDAO;
import ru.tinkoff.zeppelin.storage.SystemEventType.ET;
import ru.tinkoff.zeppelin.storage.ZLog;

/**
 * Class for handle ready for execute jobs
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
@Component
public class ExecutionHandler extends AbstractHandler {

  public ExecutionHandler(final JobBatchDAO jobBatchDAO,
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
  public void run(final Note note, final List<Paragraph> paragraphs, final String username, final Set<String> roles) {
    if (noteIsRunning(note)) {
      if (paragraphs.size() != 1) {
        return;
      }

      final JobBatch jobBatch = jobBatchDAO.get(note.getBatchJobId());
      final List<Job> jobs = jobDAO.loadByBatch(jobBatch.getId());
      if (!jobs.get(0).getUsername().equals(username)) {
        return;
      }

      final Paragraph paragraph = paragraphs.get(0);
      final boolean contains = jobs.stream().anyMatch(j -> j.getParagraphId() == paragraph.getId());
      if (contains) {
        return;
      }
      appendJob(jobBatch, note, paragraph, jobs.size(), JobPriority.USER.getIndex(), username, roles);
    } else {

      ZLog.log(ET.JOB_SUBMITTED_FOR_EXECUTION,
          String.format("Задача добавлена в очередь на исполнение (ноут[id=%s], автор задачи=%s)", note.getId(), username),
          SystemEvent.SYSTEM_USERNAME);
      publishBatch(note, paragraphs, username, roles, JobPriority.USER.getIndex());
    }
  }
}

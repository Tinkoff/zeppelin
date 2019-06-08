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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import org.quartz.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.SystemEvent;
import ru.tinkoff.zeppelin.core.notebook.JobPriority;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.core.notebook.Scheduler;
import ru.tinkoff.zeppelin.engine.NoteEventService;
import ru.tinkoff.zeppelin.storage.FullParagraphDAO;
import ru.tinkoff.zeppelin.storage.JobBatchDAO;
import ru.tinkoff.zeppelin.storage.JobDAO;
import ru.tinkoff.zeppelin.storage.JobPayloadDAO;
import ru.tinkoff.zeppelin.storage.JobResultDAO;
import ru.tinkoff.zeppelin.storage.NoteDAO;
import ru.tinkoff.zeppelin.storage.ParagraphDAO;
import ru.tinkoff.zeppelin.storage.SchedulerDAO;
import ru.tinkoff.zeppelin.storage.SystemEventType.ET;
import ru.tinkoff.zeppelin.storage.ZLog;

/**
 * Class for handle scheduled tasks
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
@Component
public class SchedulerHandler extends AbstractHandler {

  private final SchedulerDAO schedulerDAO;
  private final NoteEventService noteEventService;

  public SchedulerHandler(final JobBatchDAO jobBatchDAO,
                          final JobDAO jobDAO,
                          final JobResultDAO jobResultDAO,
                          final JobPayloadDAO jobPayloadDAO,
                          final NoteDAO noteDAO,
                          final ParagraphDAO paragraphDAO,
                          final FullParagraphDAO fullParagraphDAO,
                          final SchedulerDAO schedulerDAO,
                          final NoteEventService noteEventService) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO);
    this.schedulerDAO = schedulerDAO;
    this.noteEventService = noteEventService;
  }

  public List<Scheduler> loadJobs() {
    return schedulerDAO.getReadyToExecute(LocalDateTime.now());
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handle(final Scheduler scheduler) {
    final Note note = noteDAO.get(scheduler.getNoteId());
    final List<Paragraph> paragraphs = paragraphDAO.getByNoteId(note.getId());

    if (noteIsRunning(note)) {
      ZLog.log(ET.JOB_ALREADY_RUNNING,
          String.format("Ноут[id=%s] уже запущен и не будет выполнен по РАСПИСАНИЮ, [автор задачи=%s]",
              note.getId(), scheduler.getUser()), SystemEvent.SYSTEM_USERNAME);
      return;
    }

    ZLog.log(ET.JOB_READY_FOR_EXECUTION_BY_SCHEDULER,
        String.format("Ноут[id=%s] готов к исполнению по РАСПИСАНИЮ (автор задачи=%s)",
            scheduler.getNoteId(), scheduler.getUser()), SystemEvent.SYSTEM_USERNAME);
    noteEventService.successOnNoteScheduleExecution(note.getId());
    publishBatch(note, paragraphs, scheduler.getUser(), scheduler.getRoles(), JobPriority.SCHEDULER.getIndex());

    final CronExpression cronExpression;
    try {
      cronExpression = new CronExpression(scheduler.getExpression());
    } catch (final Exception e) {
      ZLog.log(ET.SCHEDULED_JOB_ERRORED,
          String.format("Ноут поставлен на расписание[noteId=%s] с некорректным правилом[%s]",
              scheduler.getNoteId(), scheduler.getExpression()), SystemEvent.SYSTEM_USERNAME);
      throw new IllegalArgumentException("Wrong cron expression");
    }

    final Date nextExecutionDate = cronExpression.getNextValidTimeAfter(new Date());
    final LocalDateTime nextExecution = LocalDateTime.ofInstant(nextExecutionDate.toInstant(), ZoneId.systemDefault());

    scheduler.setLastExecution(scheduler.getNextExecution());
    scheduler.setNextExecution(nextExecution);
    schedulerDAO.update(scheduler);
    ZLog.log(ET.JOB_SCHEDULED,
        String.format("Ноут успешно добавлен на выполеник по расписанию, noteId=%s, следующее время выполнения=%s", scheduler.getNoteId(), nextExecution),
        SystemEvent.SYSTEM_USERNAME);
  }

}

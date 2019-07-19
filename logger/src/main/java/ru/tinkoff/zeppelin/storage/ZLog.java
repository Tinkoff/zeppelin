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
package ru.tinkoff.zeppelin.storage;

import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.storage.SystemEventType.ET;


/**
 * Simple event logger.
 */
@Component
public class ZLog {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZLog.class);

  private final ApplicationContext applicationContext;

  private static ZLog instance;

  @Nonnull
  private final ConcurrentLinkedQueue<SystemEventDTO> eventQueue;

  @Nonnull
  private final SystemEventDAO storage;

  public ZLog(@Nonnull final SystemEventDAO storage, final ApplicationContext applicationContext) {
    this.storage = storage;
    this.applicationContext = applicationContext;
    this.eventQueue = new ConcurrentLinkedQueue<>();
  }

  @PostConstruct
  public void init() {
    instance = applicationContext.getBean(ZLog.class);
  }

  /**
   * Publish system event.
   */
  @Scheduled(fixedDelay = 5)
  private void publish() {
    while (true) {
      final SystemEventDTO event = eventQueue.poll();
      if (event == null) {
        return;
      }

      try {
        storage.persist(event);
      } catch (final Exception e) {
        LOGGER.error("Ошибка при сохранении системного события", e);
      }
    }
  }

  private void enqueue(@Nonnull final ET eventType,
                       @Nonnull final String message,
                       @Nullable final String description,
                       @Nonnull final String username,
                       @Nonnull final LocalDateTime time) {
    final SystemEventDTO event = new SystemEventDTO(username, eventType.name(), message, description, time);
    eventQueue.add(event);
  }

  public static void log(@Nonnull final ET eventType,
                         @Nonnull final String message,
                         @Nullable final String description,
                         @Nonnull final String username) {
    try {
      instance.enqueue(eventType, message, description, username, LocalDateTime.now());
    } catch (final Exception e) {
      // Logs from interpreter reinstall will be ignored because instance is null at start.
      LOGGER.error("Ошибка при добавлении системного события в очередь", e);
    }
  }

  /**
   * Record without description.
   */
  public static void log(@Nonnull final ET eventType,
                         @Nonnull final String message,
                         @Nonnull final String username) {
    log(eventType, message, null, username);
  }
}

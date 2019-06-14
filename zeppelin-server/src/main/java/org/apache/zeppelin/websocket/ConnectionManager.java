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

package org.apache.zeppelin.websocket;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.zeppelin.realm.AuthorizationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;
import ru.tinkoff.zeppelin.engine.EventService;

/**
 * Manager class for managing websocket connections
 */
@Component
public class ConnectionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

  // noteId -> connection
  private final Map<Long, Queue<WebSocketSession>> noteSocketMap = new ConcurrentHashMap<>();


  public Long getAssociatedNoteId(final WebSocketSession socket) {
    final Set<Long> noteIds = noteSocketMap.keySet();
    for (final Long noteId : noteIds) {
      final Queue<WebSocketSession> sockets = noteSocketMap.get(noteId);
      if (sockets.contains(socket)) {
        return noteId;
      }
    }
    return null;
  }

  public void addSubscriberToNode(final Long noteId, final WebSocketSession socket) {
    LOGGER.debug("Add connection {} to note: {}", socket, noteId);
    // make sure a socket relates only an single note.
    removeSubscribersFromAllNote(socket);

    final Queue<WebSocketSession> sessions = noteSocketMap.computeIfAbsent(noteId, k -> new ConcurrentLinkedQueue<>());
    if (!sessions.contains(socket)) {
      sessions.add(socket);
    }

    sendColaborativeStatus(sessions);
  }

  private void removeSubscriberFromNote(final Long noteId, final WebSocketSession socket) {
    LOGGER.debug("Remove connection {} from note: {}", socket, noteId);
    final Queue<WebSocketSession> sessions = noteSocketMap.get(noteId);
    if (sessions != null) {
      sessions.remove(socket);
      sendColaborativeStatus(sessions);
    }
  }

  private void sendColaborativeStatus(final Queue<WebSocketSession> sessions) {

    final List<String> users = new ArrayList<>();
    if (sessions.size() > 1) {
      for (final WebSocketSession session : sessions) {
        final String user = session.getPrincipal() != null
                ? session.getPrincipal().getName()
                : AuthorizationService.ANONYMOUS;
        users.add(user);
      }
    }

    for (final WebSocketSession session : sessions) {
      try {
        boolean collaborativeStatusNew = users.size() > 1;
        SockMessage message = new SockMessage(Operation.COLLABORATIVE_MODE_STATUS);
        message.put("status", collaborativeStatusNew);
        if (collaborativeStatusNew) {
          message.put("users", users);
        }
        session.sendMessage(message.toSend());
      } catch (final Exception e) {
        //SKIP
      }
    }
  }

  public void removeNoteSubscribers(final Long noteId) {
    noteSocketMap.remove(noteId);
  }

  public void removeSubscribersFromAllNote(final WebSocketSession socket) {
    final Set<Long> noteIds = noteSocketMap.keySet();
    for (final Long noteId : noteIds) {
      removeSubscriberFromNote(noteId, socket);
    }
  }

  public void broadcast(final Long noteId, final SockMessage m) {
    final Queue<WebSocketSession> sessions = noteSocketMap.get(noteId);
    if (sessions == null || sessions.size() == 0) {
      return;
    }
    LOGGER.debug("SEND >> " + m);
    for (final WebSocketSession session : sessions) {
      int i = 5;
      while (i > 0) {
        try {
          i--;
          session.sendMessage(m.toSend());
          break;
        } catch (final Exception e) {
          LOGGER.error("Send error: " + m, e);
          try {
            Thread.sleep(10);
          } catch (final Exception e1) {
            //SKIP
          }
        }
      }
    }
  }

  @Scheduled(fixedDelay = 5)
  private void getEvents() {
    while (true) {
      final EventService.Event event = EventService.getEvent();
      if (event == null) {
        return;
      }

      if (event.getBefore() != null && event.getAfter() != null) {
        broadcast(
                event.getNoteId(),
                new SockMessage(Operation.PARAGRAPH)
                        .put("paragraph", event.getAfter())
        );
      } else if (event.getBefore() == null && event.getAfter() != null) {
        broadcast(
                event.getNoteId(),
                new SockMessage(Operation.PARAGRAPH_ADDED)
                        .put("paragraph", event.getAfter())
                        .put("index", event.getAfter().getPosition())
        );
      } else if (event.getBefore() != null && event.getAfter() == null) {
        broadcast(
                event.getNoteId(),
                new SockMessage(Operation.PARAGRAPH_REMOVED)
                        .put("id", event.getBefore().getId())
                        .put("index", event.getBefore().getPosition())
        );
      }
    }
  }
}

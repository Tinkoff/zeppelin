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

package org.apache.zeppelin.websocket.handler;

import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;
import ru.tinkoff.zeppelin.engine.NoteService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Component
public class SettingsHandler extends AbstractHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(SettingsHandler.class);

  @Autowired
  public SettingsHandler(final NoteService noteService,
                         final ConnectionManager connectionManager) {
    super(connectionManager, noteService);
  }

  public void sendAllConfigurations(final WebSocketSession conn) throws IOException {
    final Map<String, String> properties = new HashMap<>();

    final SockMessage message = new SockMessage(Operation.CONFIGURATIONS_INFO)
            .put("configurations", properties);
    LOGGER.info("Передача всех значений конфигурации");
    conn.sendMessage(message.toSend());
  }
}

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.rest;


import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.core.externalDTO.InterpreterQueueDTO;
import ru.tinkoff.zeppelin.storage.InterpreterQueueDAO;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/interpreter_queue")
public class InterpreterQueueRestAPI {

  private final InterpreterQueueDAO interpreterQueueDAO;

  @Autowired
  public InterpreterQueueRestAPI(final InterpreterQueueDAO interpreterQueueDAO) {
    this.interpreterQueueDAO = interpreterQueueDAO;
  }

  @GetMapping()
  public ResponseEntity getInterpreterQueue() {
    try {
      return new JsonResponse(HttpStatus.OK, "", interpreterQueueDAO.getInterpreterQueue()).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @GetMapping(value = "/{noteUuid}", produces = "application/json")
  public ResponseEntity getNoteQueue(@PathVariable("noteUuid") final String noteUuid) {
    try {
      final Map<String, List<String>> result = new HashMap<>();
      interpreterQueueDAO.getInterpreterQueue()
              .forEach((k, v) ->
                      result.put(k, v.stream().map(InterpreterQueueDTO::getNoteUuid).collect(Collectors.toList()))
              );

      final JsonObject response = new JsonObject();
      response.add("interpreter", new JsonArray());
      result.forEach((key, value) -> {
        if (value.indexOf(noteUuid) != -1) {
          final JsonObject obj = new JsonObject();
          obj.addProperty("name", key);
          obj.addProperty("queueSize", value.size());
          obj.addProperty("queuePosition", value.indexOf(noteUuid) + 1);
          response.getAsJsonArray("interpreter").add(obj);
        }
      });

      return new JsonResponse(HttpStatus.OK, "", response).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }
}
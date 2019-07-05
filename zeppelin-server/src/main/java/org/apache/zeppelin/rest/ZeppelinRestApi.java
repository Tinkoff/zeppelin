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

import java.util.HashMap;
import java.util.Map;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.engine.Configuration;

/**
 * Zeppelin root rest api endpoint.
 *
 * @since 0.3.4
 */
@RestController
@RequestMapping("/api/")
public class ZeppelinRestApi {

  /**
   * Get the root endpoint Return always 200.
   *
   * @return 200 response
   */
  @GetMapping(produces = "application/json")
  public ResponseEntity getRoot() {
    return new JsonResponse(HttpStatus.OK).build();
  }


  @GetMapping(value = "/version", produces = "application/json")
  public ResponseEntity getVersion() {
    final Map<String, String> versionInfo = new HashMap<>();
    versionInfo.put("version", Util.getVersion());
    versionInfo.put("hash", Util.getGitCommitId());
    versionInfo.put("branch", Util.getGitBranch());
    versionInfo.put("time", Util.getBuildTimestamp());

    return new JsonResponse(HttpStatus.OK, "Zeppelin version", versionInfo).build();
  }

  @GetMapping("/metadata_server_url")
  public ResponseEntity getMetaserverUrl() {
    String url = Configuration.getMetaserverLocation();
    return new JsonResponse(HttpStatus.OK, url).build();
  }
}

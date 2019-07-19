/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace java ru.tinkoff.zeppelin.interpreter.thrift

enum PingResultStatus {
    OK,
    KILL_ME
}

struct PingResult {
  1: PingResultStatus status,
  2: string uuid
}

service RemoteProcessThriftService {

  PingResult ping();

  void shutdown();
}


enum PushResultStatus {
    ACCEPT,
    DECLINE,
    ERROR
}

struct PushResult {
  1: PushResultStatus status,
  2: string interpreterJobUUID,
  3: string interpreterProcessUUID
}

enum CancelResultStatus {
    ACCEPT,
    NOT_FOUND,
    ERROR
}

struct CancelResult {
  1: CancelResultStatus status,
  2: string interpreterJobUUID,
  3: string uuid
}

enum FinishResultStatus {
     ACCEPT,
     NOT_FOUND,
     ERROR
}

service RemoteInterpreterThriftService extends RemoteProcessThriftService {

  PushResult push(1: string st, 2: string contextJson);
  FinishResultStatus finish(1: string contextJson);
  CancelResult cancel(1: string UUID);
}


service RemoteCompleterThriftService extends RemoteProcessThriftService {

  string compete(1: string st, 2: i32 cursorPosition, 3: map<string, string> noteContext, 4: map<string, string> userContext, 5:  map<string, string> configuration);
}



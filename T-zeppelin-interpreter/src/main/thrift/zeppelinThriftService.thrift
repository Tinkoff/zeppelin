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

struct RegisterInfo {
  1: string host,
  2: i32 port
  3: string shebang
  4: string processType
  5: string processUUID
}

enum RunNoteResultStatus {
    ACCEPT,
    ERROR
}

struct RunNoteResult {
  1: RunNoteResultStatus status,
  2: i64 batchId,
  3: string message
}

enum AbortBatchResultStatus {
    ACCEPT,
    ERROR
}

struct AbortBatchResult {
  1: AbortBatchResultStatus status,
  2: string message
}

enum BatchResultStatus {
    ACCEPT,
    ERROR
}

enum BatchStatus {
    DONE,
    RUNNING,
    ERROR
}

struct BatchStatusResult {
  1: BatchResultStatus status,
  2: BatchStatus batchStatus,
  3: string message
}

service ZeppelinThriftService {

  void registerInterpreterProcess(1: RegisterInfo registerInfo);

  void handleInterpreterResult(1: string UUID, 2: string payload);

  void handleInterpreterTempOutput(1: string UUID, 2: string tempOutput);

  RunNoteResult handleRunNote(1: string noteUUID, 2: string userName, 3: set<string> userGroups);
  AbortBatchResult handleAbortBatch(1: i64 batchId, 2: string userName, 3: set<string> userGroups);
  BatchStatusResult handleGetBatchStatus(1: i64 batchId, 2: string userName, 3: set<string> userGroups);

}
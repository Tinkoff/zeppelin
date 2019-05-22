--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

-- Represents ru.tinkoff.zeppelin.storage.SystemEventType.ET
CREATE TABLE SYSTEM_EVENT_TYPE
(
  ID    BIGSERIAL     PRIMARY KEY,
  NAME  VARCHAR(255)  NOT NULL UNIQUE
);

INSERT INTO SYSTEM_EVENT_TYPE(name)
VALUES ('UI_EVENT'),
       ('INTERPRETER_PROCESS_NOT_FOUND'),
       ('JOB_CANCEL_FAILED'),
       ('JOB_CANCEL_ACCEPTED'),
       ('JOB_CANCEL_NOT_FOUND'),
       ('JOB_CANCEL_ERRORED'),
       ('JOB_ALREADY_RUNNING'),
       ('JOB_SUBMITTED_FOR_EXECUTION'),
       ('GOT_JOB'),
       ('GOT_ABORTED_BATCH'),
       ('MODULE_INSTALL'),
       ('MODULE_ALREADY_INSTALLED'),
       ('MODULE_SUCCESSFULLY_INSTALLED'),
       ('MODULE_INSTALLATION_FAILED'),
       ('MODULE_SUCCESSFULLY_UNINSTALLED'),
       ('MODULE_DELETION_FAILED'),
       ('MODULE_CONFIGURATION_REQUESTED'),
       ('MODULE_CONFIGURAION_FOUND'),
       ('MODULE_CONFIGURATION_PROCESSING_FAILED'),
       ('PROCESS_STARTED'),
       ('REMOTE_CONNECTION_REGISTERED'),
       ('BAD_REMOTE_CONNECTION'),
       ('COMPLETED_PROCESS_NOT_FOUND'),
       ('PROCESS_COMPLETED'),
       ('CONNECTION_FAILED'),
       ('FAILED_TO_RELEASE_CONNECTION'),
       ('FORCE_KILL_REQUESTED'),
       ('PUSH_FAILED_CLIENT_NOT_FOUND'),
       ('PUSH_FAILED'),
       ('PING_FAILED_CLIENT_NOT_FOUND'),
       ('PING_FAILED'),
       ('FORCE_KILL_FAILED_CLIENT_NOT_FOUND'),
       ('FORCE_KILL_FAILED'),
       ('REMOTE_PROCESS_SERVER_STARTING'),
       ('REMOTE_PROCESS_SERVER_START_FAILED'),
       ('REMOTE_PROCESS_SERVER_STARTED'),
       ('REMOTE_PROCESS_SERVER_STOPPED'),
       ('REMOTE_PROCESS_START_REQUESTED'),
       ('REMOTE_PROCESS_FINISHED'),
       ('REMOTE_PROCESS_FAILED'),
       ('JOB_READY_FOR_EXECUTION'),
       ('JOB_REQUEST_IS_EMPTY'),
       ('JOB_ACCEPTED'),
       ('JOB_DECLINED'),
       ('JOB_REQUEST_ERRORED'),
       ('JOB_UNDEFINED'),
       ('JOB_READY_FOR_EXECUTION_BY_SCHEDULER'),
       ('SCHEDULED_JOB_ERRORED'),
       ('JOB_SCHEDULED'),
       ('JOB_NOT_FOUND'),
       ('INTERPRETER_RESULT_NOT_FOUND'),
       ('SUCCESSFUL_RESULT'),
       ('ABORTED_RESULT'),
       ('ERRORED_RESULT'),
       ('ACCESS_ERROR'),
       ('FAILED_TO_ADD_SYSTEM_EVENT'),
       ('FAILED_TO_SAVE_EVENT'),
       ('NOTE_OPENED'),
       ('NOTE_DELETED'),
       ('NOTE_CREATED'),
       ('FAILED_TO_CREATE_NOTE'),
       ('NOTE_RESTORED'),
       ('USER_POST_LOGIN'),
       ('USER_LOGOUT'),
       ('USER_CONNECTED');

-- Represents org.apache.zeppelin.SystemEvent
CREATE TABLE SYSTEM_EVENT
(
  ID            BIGSERIAL    PRIMARY KEY,
  USERNAME      VARCHAR(255) NOT NULL,
  EVENT_TYPE    BIGINT       REFERENCES SYSTEM_EVENT_TYPE (ID) ON UPDATE CASCADE,
  MESSAGE       TEXT         NOT NULL,
  DESCRIPTION   TEXT,
  ACTION_TIME   TIMESTAMP    NOT NULL
);

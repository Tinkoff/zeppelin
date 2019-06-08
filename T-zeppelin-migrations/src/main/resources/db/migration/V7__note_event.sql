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

CREATE TABLE NOTE_EVENT_TYPES
(
  ID            BIGSERIAL       PRIMARY KEY,
  TYPE          VARCHAR(25)     NOT NULL,
  DESCRIPTION   VARCHAR(200)    NOT NULL,
  UNIQUE (TYPE)
);

INSERT INTO NOTE_EVENT_TYPES
    (ID,
    TYPE,
    DESCRIPTION)
VALUES  (1,'RUN', 'NOTE RUN ON SCHEDULE EVENT'),
        (2,'ERROR', 'NOTE EXECUTION ON SCHEDULE ERROR  EVENT'),
        (3,'SCHEDULE_CHANGE', 'NOTE SCHEDULE CHANGE EVENT');

CREATE TABLE NOTE_EVENT_NOTIFICATIONS
(
  ID                    BIGSERIAL       PRIMARY KEY,
  NOTIFICATION          VARCHAR(25)     NOT NULL,
  DESCRIPTION           VARCHAR(200)    NOT NULL,
  UNIQUE (NOTIFICATION)
);

INSERT INTO NOTE_EVENT_NOTIFICATIONS
    (ID,
    NOTIFICATION,
    DESCRIPTION)
VALUES  (1,'EMAIL', 'EMAIL NOTIFICATION');


CREATE TABLE NOTE_EVENT_SUBSCRIBERS
(
  ID                BIGSERIAL    PRIMARY KEY,
  USER_NAME         VARCHAR(200) NOT NULL,
  EVENT_TYPE        VARCHAR(25)  REFERENCES NOTE_EVENT_TYPES (TYPE) ON DELETE CASCADE NOT NULL,
  NOTE_ID           BIGINT REFERENCES NOTES (ID) ON DELETE CASCADE NOT NULL,
  NOTIFICATION_TYPE VARCHAR(25)  REFERENCES NOTE_EVENT_NOTIFICATIONS (NOTIFICATION) ON DELETE CASCADE NOT NULL,
  SCHEDULER_ID BIGINT  REFERENCES SCHEDULER (ID) ON DELETE CASCADE NOT NULL,
  UNIQUE (USER_NAME, NOTE_ID, EVENT_TYPE, NOTIFICATION_TYPE)
);
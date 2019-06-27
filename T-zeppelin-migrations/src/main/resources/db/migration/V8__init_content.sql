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

-- Represents ru.tinkoff.zeppelin.core.content.ContentType
CREATE TABLE CONTENT_TYPE
(
    ID   BIGSERIAL PRIMARY KEY,
    TYPE VARCHAR(25) NOT NULL,
    UNIQUE (TYPE)
);

INSERT INTO CONTENT_TYPE
    (TYPE)
VALUES ('TABLE'),
       ('FILE');

-- Represents ru.tinkoff.zeppelin.core.content.Content
CREATE TABLE CONTENT
(
    ID          BIGSERIAL PRIMARY KEY,
    NOTE_ID     BIGINT REFERENCES NOTES (ID) ON DELETE CASCADE               NOT NULL,
    TYPE        VARCHAR(25) REFERENCES CONTENT_TYPE (TYPE) ON DELETE CASCADE NOT NULL,
    DESCRIPTION VARCHAR(1024),
    LOCATION    VARCHAR(1024) UNIQUE
);

CREATE INDEX location_idx ON CONTENT (LOCATION);

-- Represents ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration
CREATE TABLE CONTENT_TO_PARAGRAPH
(
    CONTENT_ID   BIGINT REFERENCES CONTENT (ID) ON DELETE CASCADE    NOT NULL,
    PARAGRAPH_ID BIGINT REFERENCES PARAGRAPHS (ID) ON DELETE CASCADE NOT NULL,
    UNIQUE (CONTENT_ID, PARAGRAPH_ID)
);

CREATE TABLE CONTENT_PARAMS
(
    NAME       VARCHAR(30)                                      NOT NULL,
    CONTENT_ID BIGINT REFERENCES CONTENT (ID) ON DELETE CASCADE NOT NULL,
    VALUE      JSON                                             NOT NULL,
    UNIQUE (CONTENT_ID,NAME)
);
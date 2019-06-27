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
CREATE TABLE BASE_CONFIGURATION_TYPES
(
  ID          BIGSERIAL    PRIMARY KEY,
  TYPE    VARCHAR(200)   NOT NULL,
  UNIQUE (TYPE)
);

INSERT INTO BASE_CONFIGURATION_TYPES
    (ID,
    TYPE)
VALUES  (1, 'NUMBER'),
        (2, 'STRING'),
        (3, 'PASSWORD'),
        (4, 'BOOLEAN');

CREATE TABLE BASE_CONFIGURATION
(
  ID          BIGSERIAL    PRIMARY KEY,
  NAME    VARCHAR(200)   NOT NULL,
  VALUE   VARCHAR(200)   NOT NULL,
  TYPE    VARCHAR(200)   REFERENCES BASE_CONFIGURATION_TYPES (TYPE) ON DELETE CASCADE NOT NULL,
  DESCRIPTION VARCHAR(200) NOT NULL,
  UNIQUE (NAME)
);

INSERT INTO BASE_CONFIGURATION
    (ID,
    NAME,
    VALUE,
    TYPE,
    DESCRIPTION)
VALUES  (1,'SCHEDULER_ENABLE_FLAG', 'true', 'BOOLEAN','Scheduler enable/disable flag');
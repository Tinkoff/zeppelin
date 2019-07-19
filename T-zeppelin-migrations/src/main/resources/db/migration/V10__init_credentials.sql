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

-- represents ru.tinkoff.zeppelin.core.Credential
CREATE TABLE CREDENTIALS
(
  ID          BIGSERIAL PRIMARY KEY,
  KEY         VARCHAR(255)  NOT NULL UNIQUE CHECK (KEY ~* 'Z_ENV_CREDENTIAL_.+$'),
  VALUE       VARCHAR(2048) NOT NULL,
  SECURE      BOOLEAN       NOT NULL DEFAULT FALSE,
  DESCRIPTION VARCHAR(2048),
  PERMISSIONS JSON          NOT NULL DEFAULT '{"owners": [], "readers": []}'::JSON
);

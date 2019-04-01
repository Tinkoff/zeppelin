CREATE TABLE notes
(
  id          BIGSERIAL PRIMARY KEY,
  note_id     VARCHAR(9)    NOT NULL UNIQUE,
  path        VARCHAR(1024) NOT NULL,
  permissions JSON          NOT NULL,
  gui         JSON          NOT NULL
);

CREATE TABLE revisions
(
  id      BIGSERIAL PRIMARY KEY,
  note_id BIGINT REFERENCES notes (id) ON DELETE CASCADE,
  message VARCHAR(128) NOT NULL,
  date    TIMESTAMP    NOT NULL
);

CREATE TABLE paragraphs
(
  id           BIGSERIAL PRIMARY KEY,
  paragraph_id VARCHAR(40)  NOT NULL,
  db_note_id   BIGINT REFERENCES notes (id) ON DELETE CASCADE,
  revision_id  BIGINT REFERENCES revisions (id),
  job_id       BIGINT,
  title        VARCHAR(256) NOT NULL,
  text         TEXT         NOT NULL,
  username     VARCHAR(128) NOT NULL,
  created      TIMESTAMP    NOT NULL,
  updated      TIMESTAMP    NOT NULL,
  config       JSON         NOT NULL,
  gui          JSON         NOT NULL,
  position     INTEGER      NOT NULL,
  job          BIGINT,
  shebang      VARCHAR(128)
);

CREATE TABLE JOB_BATCH
(
  ID         BIGSERIAL PRIMARY KEY,
  NOTE_ID    BIGINT      NOT NULL REFERENCES NOTES (ID) ON DELETE CASCADE,
  STATUS     VARCHAR(50) NOT NULL,
  CREATED_AT TIMESTAMP   NOT NULL,
  STARTED_AT TIMESTAMP,
  ENDED_AT   TIMESTAMP
);

CREATE TABLE JOB
(
  ID                       BIGSERIAL PRIMARY KEY,
  BATCH_ID                 BIGINT REFERENCES JOB_BATCH (ID) ON DELETE CASCADE,
  NOTE_ID                  BIGINT REFERENCES NOTES (ID) ON DELETE CASCADE,
  PARAGRAPH_ID             BIGINT REFERENCES PARAGRAPHS (ID) ON DELETE CASCADE,
  INDEX_NUMBER             INTEGER      NOT NULL,
  SHEBANG                  VARCHAR(100) NOT NULL,
  STATUS                   VARCHAR(50)  NOT NULL,
  INTERPRETER_PROCESS_UUID VARCHAR(200),
  INTERPRETER_JOB_UUID     VARCHAR(200),
  CREATED_AT               TIMESTAMP,
  STARTED_AT               TIMESTAMP,
  ENDED_AT                 TIMESTAMP
);

ALTER TABLE paragraphs
  ADD FOREIGN KEY (job_id) REFERENCES JOB (ID);

CREATE TABLE JOB_PAYLOAD
(
  ID      BIGSERIAL PRIMARY KEY,
  JOB_ID  BIGINT REFERENCES JOB (ID) ON DELETE CASCADE,
  PAYLOAD TEXT
);


CREATE TABLE JOB_RESULT
(
  ID         BIGSERIAL PRIMARY KEY,
  JOB_ID     BIGINT REFERENCES JOB (ID) ON DELETE CASCADE,
  CREATED_AT TIMESTAMP,
  TYPE       VARCHAR(50),
  RESULT     TEXT
);

ALTER TABLE paragraphs
  ADD FOREIGN KEY (job) REFERENCES JOB (ID);

CREATE TABLE SCHEDULER
(
  ID                       BIGSERIAL PRIMARY KEY,
  NOTE_ID                  BIGINT REFERENCES NOTES (ID) ON DELETE CASCADE,
  ENABLED                  VARCHAR(10) NOT NULL DEFAULT 'false',
  EXPRESSION               VARCHAR(100) NOT NULL,
  USER_NAME                VARCHAR(200) NOT NULL,
  USER_ROLES               VARCHAR(2048),
  LAST_EXECUTION           TIMESTAMP NOT NULL,
  NEXT_EXECUTION           TIMESTAMP NOT NULL
);


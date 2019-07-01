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
package ru.tinkoff.zeppelin.storage;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.tinkoff.zeppelin.core.notebook.Note;

import static org.junit.Assert.assertNotNull;

public abstract class AbstractTest {
  private static final Logger log = LoggerFactory.getLogger(AbstractTest.class);

  static NamedParameterJdbcTemplate jdbcTemplate;
  private static final NoteDAO noteDAO;

  static {
    final EmbeddedPostgres embeddedPostgres = getNewEmbeddedPostgres();
    assertNotNull(embeddedPostgres);
    jdbcTemplate = new NamedParameterJdbcTemplate(embeddedPostgres.getTemplateDatabase());
    noteDAO = new NoteDAO(jdbcTemplate);
  }

  private static EmbeddedPostgres getNewEmbeddedPostgres() {
    try {
      final EmbeddedPostgres embeddedPostgres = EmbeddedPostgres.builder().start();

      // init migration
      Flyway.configure().dataSource(embeddedPostgres.getTemplateDatabase())
          .locations("classpath:db.migration").load().migrate();

      return embeddedPostgres;
    } catch (final Exception e) {
      log.error("Can't create 'Embedded Postgres'", e);
      return null;
    }
  }

  static Note createNote(final String path, final String UUID) {
    final Note note = new Note(path);
    note.setUuid(UUID);
    noteDAO.persist(note);
    return note;
  }

  static Note getNote(final String UUID) {
    return noteDAO.get(UUID);
  }

  static void deleteNote(final String UUID) {
    try {
      noteDAO.remove(noteDAO.get(UUID));
    } catch (final Exception e) {
      // ignore
    }
  }

  @SuppressWarnings("unused")
  class Person {
    final static String USERNAME_1 = "notsavelenko";
    final static String USERNAME_2 = "notklimov";
    final static String USERNAME_3 = "notkoshin";
    final static String USERNAME_4 = "notschurova";
  }

  @SuppressWarnings("unused")
  class Notes {
    final static String UUID_1 = "2EEFSRFR1";
    final static String UUID_2 = "2EED49JF2";
    final static String UUID_3 = "2E46LSF33";
    final static String UUID_4 = "2ESGER4G4";
    final static String UUID_5 = "2EG5NE5Y5";
    final static String UUID_6 = "2EH64HE56";
  }
}

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

import com.google.gson.Gson;
import java.sql.SQLException;
import com.google.gson.JsonElement;
import org.postgresql.util.PGobject;

class Utils {
  private static final Gson gson = new Gson();

  static PGobject generatePGjson(final Object value) {
    try {
      final PGobject pgObject = new PGobject();
      pgObject.setType("jsonb");
      pgObject.setValue(gson.toJson(value));
      return pgObject;
    } catch (final SQLException e) {
      throw new RuntimeException("Can't generate postgres json", e);
    }
  }

  static PGobject getPGjson(final JsonElement value) {
    try {
      final PGobject pgObject = new PGobject();
      pgObject.setType("jsonb");
      pgObject.setValue(gson.toJson(value));
      return pgObject;
    } catch (final SQLException e) {
      throw new RuntimeException("Can't generate postgres json", e);
    }
  }
}

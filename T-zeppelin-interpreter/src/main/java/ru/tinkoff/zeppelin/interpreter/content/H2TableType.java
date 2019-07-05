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
package ru.tinkoff.zeppelin.interpreter.content;

import java.util.Arrays;

public enum  H2TableType {

  REAL("R_TABLES"),
  VIRTUAL("V_TABLES"),
  SELECT("S_TABLES"),
  ;

  private final String schemaName;

  H2TableType(final String schemaName) {
    this.schemaName = schemaName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public static H2TableType fromSchemaName(final String schemaName) {
   return Arrays.stream(H2TableType.values())
           .filter(t -> t.getSchemaName().equals(schemaName))
           .findFirst()
           .orElseThrow(() -> new RuntimeException("Unknown type"));
  }
}

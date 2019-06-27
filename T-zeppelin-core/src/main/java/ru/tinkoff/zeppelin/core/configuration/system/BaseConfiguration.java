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
package ru.tinkoff.zeppelin.core.configuration.system;

public class BaseConfiguration<T> {
  public enum Type {
    NUMBER,
    STRING,
    BOOLEAN,
    PASSWORD;

    public static Object getDefault(final Type type) {
      switch (type) {
        case NUMBER:
          return 0;
        case BOOLEAN:
          return false;
        default:
          return "";
      }
    }
  }

  private final Long id;
  private final String name;
  private final T value;
  private final Type valueType;
  private final String description;

  public BaseConfiguration(final Long id,
                           final String name,
                           final T value,
                           final Type valueType,
                           final String description) {
    this.id = id;
    this.name = name;
    this.value = value;
    this.valueType = valueType;
    this.description = description;
  }

  public String getName() {
    return name;
  }

  @SuppressWarnings("unchecked")
  public T value() {
    if (value != null) {
      return value;
    }
    return (T) Type.getDefault(valueType);
  }

  public String getDescription() {
    return description;
  }
}

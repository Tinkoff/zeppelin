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
package ru.tinkoff.zeppelin.interpreter.jdbc.simple;

import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import ru.tinkoff.zeppelin.commons.jdbc.AbstractJDBCInterpreter;

/**
 * Common JDBC Interpreter: This interpreter can be used for accessing different SQL databases.
 * <p>
 * Before using interpreter you should configure driver, which will be installed at runtime:
 * <ul>
 * <li>{@code driver.className} - driver class name, e.g. {@code org.postgresql.Driver}</li>
 * <li>{@code driver.artifact} - maven driver artifact, e.g. {@code org.postgresql:postgresql:jar:42.2.5}</li>
 * </ul>
 * <p>
 * Specify remoteConnection:
 * <ul>
 * <li>{@code remoteConnection.user} - username for database remoteConnection</li>
 * <li>{@code remoteConnection.url} - database url</li>
 * <li>{@code remoteConnection.password} - password</li>
 * </ul>
 * <p>
 * Precode and Postcode rules:
 * <ul>
 * <li>If precode fails -> Error result from precode will be returned as total remoteStatement result</li>
 * <li>If precode succeed, postcode always will be executed</li>
 * <li>If postcode fails -> error will be logged, and remoteConnection will be closed.</li>
 * <li></li>
 * </ul>
 */
public class JDBCSimpleInterpreter extends AbstractJDBCInterpreter {

    @Override
    public Iterator<String> getStatementIterator(@Nonnull final String statements) {
        return Arrays.stream(statements.split(";"))
                .filter(s -> !s.trim().isEmpty())
                .collect(Collectors.toList())
                .iterator();
    }
}


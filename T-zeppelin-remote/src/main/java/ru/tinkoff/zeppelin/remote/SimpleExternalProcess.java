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

package ru.tinkoff.zeppelin.remote;

import org.apache.commons.cli.*;
import sun.misc.Signal;

import java.util.Objects;

public class SimpleExternalProcess {

  public static void main(String[] args) throws Exception {

    final Options options = new Options();

    final Option processThread = new Option("pt", "processThread", true, "process thread class name");
    processThread.setRequired(true);
    options.addOption(processThread);

    final Option host = new Option("h", "host", true, "zeppelin thrift server host");
    host.setRequired(true);
    options.addOption(host);

    final Option port = new Option("p", "port", true, "zeppelin thrift server port");
    port.setRequired(true);
    options.addOption(port);

    final Option shebang = new Option("sb", "shebang", true, "process shebang");
    shebang.setRequired(true);
    options.addOption(shebang);

    final Option type = new Option("tp", "type", true, "process type");
    type.setRequired(true);
    options.addOption(type);

    final CommandLineParser parser = new DefaultParser();
    final HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd = null;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("utility-name", options);
    }

    if (Objects.isNull(cmd)) {
      System.exit(1);
    }

    final String processThreadClass = cmd.getOptionValue("processThread");
    final String zeppelinServerHost = cmd.getOptionValue("host");
    final String zeppelinServerPort = cmd.getOptionValue("port");
    final String processShebang = cmd.getOptionValue("shebang");
    final String processType = cmd.getOptionValue("type");

    final Class clazz = Class.forName(processThreadClass);
    final AbstractRemoteProcessThread thread = (AbstractRemoteProcessThread) clazz.newInstance();
    thread.init(
            zeppelinServerHost,
            zeppelinServerPort,
            processShebang,
            processType
    );

    Signal.handle(new Signal("TERM"), signal -> {
      thread.shutdown();
      System.exit(0);
    });

    thread.start();
    thread.join();
    System.exit(0);
  }

}

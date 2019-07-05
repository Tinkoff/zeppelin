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

package ru.tinkoff.zeppelin.engine;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("configuration")
public class Configuration {

  private final Set<String> adminUsers;
  private final Set<String> adminGroups;


  private final Set<String> defaultReaders;
  private final Set<String> defaultWriters;
  private final Set<String> defaultRunners;
  private final Set<String> defaultOwners;

  private final String thriftAddress;
  private final int thriftPort;

  private final String instanceMarkerPrefix;
  private final String metaserverLocation;
  private final String homeNodeId;

  private final boolean isCronEnable;

  private static Configuration instance;

  private Configuration(@Value("${zeppelin.admin_users}") final String admin_users,
                        @Value("${zeppelin.admin_group}") final String admin_group,
                        @Value("${zeppelin.thrift.address}") final String thriftAddress,
                        @Value("${zeppelin.thrift.port}") final int thriftPort,
                        @Value("${zeppelin.instance.markerPrefix}") final String instanceMarkerPrefix,
                        @Value("${zeppelin.metaserver.address}") final String metaserverLocation,
                        @Value("${zeppelin.home_node}") final String homeNodeId,
                        @Value("${zeppelin.note.defaultReaders}") final String defaultReaders,
                        @Value("${zeppelin.note.defaultWriters}") final String defaultWriters,
                        @Value("${zeppelin.note.defaultRunners}") final String defaultRunners,
                        @Value("${zeppelin.note.defaultOwners}") final String defaultOwners,
                        @Value("${zeppelin.cron.enable:true}") final boolean isCronEnable
                        ) {
    this.adminUsers = parseString(admin_users, ",");
    this.adminGroups = parseString(admin_group, ",");
    this.thriftAddress = thriftAddress;
    this.thriftPort = thriftPort;
    this.instanceMarkerPrefix = instanceMarkerPrefix;
    this.metaserverLocation = metaserverLocation;
    this.homeNodeId = homeNodeId;
    this.defaultReaders = parseString(defaultReaders, ",");
    this.defaultWriters = parseString(defaultWriters, ",");
    this.defaultRunners = parseString(defaultRunners, ",");
    this.defaultOwners = parseString(defaultOwners, ",");
    this.isCronEnable = isCronEnable;
    instance = this;
  }

  @PostConstruct
  private void init() {
    instance = this;
  }


  public static Set<String> getAdminUsers() {
    return instance.adminUsers;
  }

  public static Set<String> getAdminGroups() {
    return instance.adminGroups;
  }

  public static String getThriftAddress() {
    return instance.thriftAddress;
  }

  public static int getThriftPort() {
    return instance.thriftPort;
  }

  public static String getInstanceMarkerPrefix() {
    return instance.instanceMarkerPrefix;
  }

  public static String getMetaserverLocation() {
    return instance.metaserverLocation;
  }

  public static String getHomeNodeId() {
    return instance.homeNodeId;
  }

  public static Set<String> getDefaultReaders() { return instance.defaultReaders; }

  public static Set<String> getDefaultWriters() { return instance.defaultWriters; }

  public static Set<String> getDefaultRunners() { return instance.defaultRunners; }

  public static Set<String> getDefaultOwners() { return instance.defaultOwners; }

  public static boolean isCronEnable() { return instance.isCronEnable; }

  private Set<String> parseString(final String param, final String delimeter) {
    return Arrays.stream(param.split(delimeter)).map(String::trim).collect(Collectors.toSet());
  }
}

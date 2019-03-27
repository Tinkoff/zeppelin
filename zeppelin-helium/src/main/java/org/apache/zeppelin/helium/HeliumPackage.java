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
package org.apache.zeppelin.helium;

import com.google.gson.Gson;
import org.apache.zeppelin.annotation.Experimental;

import java.util.Map;

/**
 * Helium package definition
 */
@Experimental
public class HeliumPackage {
  private static final Gson gson = new Gson();

  private HeliumType type;
  private String name;           // user friendly name of this application
  private String description;    // description
  private String artifact;       // artifact name e.g) groupId:artifactId:versionId
  private String className;      // entry point
  // resource classnames that requires [[ .. and .. and .. ] or [ .. and .. and ..] ..]
  private String [][] resources;

  private String license;
  private String icon;
  private String published;

  private String groupId;        // get groupId of INTERPRETER type package
  private String artifactId;     // get artifactId of INTERPRETER type package

  private SpellPackageInfo spell;
  private Map<String, Object> config;

  public HeliumPackage(HeliumType type,
                       String name,
                       String description,
                       String artifact,
                       String className,
                       String[][] resources,
                       String license,
                       String icon) {
    this.type = type;
    this.name = name;
    this.description = description;
    this.artifact = artifact;
    this.className = className;
    this.resources = resources;
    this.license = license;
    this.icon = icon;
  }

  @Override
  public int hashCode() {
    return (type.toString() + artifact + className).hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof HeliumPackage)) {
      return false;
    }

    HeliumPackage info = (HeliumPackage) o;
    return type == info.type && artifact.equals(info.artifact) && className.equals(info.className);
  }

  public HeliumType getType() {
    return type;
  }

  public static boolean isBundleType(HeliumType type) {
    return (type == HeliumType.VISUALIZATION ||
        type == HeliumType.SPELL);
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getArtifact() {
    return artifact;
  }

  public String getClassName() {
    return className;
  }

  public String[][] getResources() {
    return resources;
  }

  public String getLicense() {
    return license;
  }

  public String getIcon() {
    return icon;
  }

  public String getPublishedDate() {
    return published;
  }

  public String getGroupId() {
    return groupId;
  }

  public String getArtifactId() {
    return artifactId;
  }

  public SpellPackageInfo getSpellInfo() {
    return spell;
  }

  public Map<String, Object> getConfig() { return config; }

  public HeliumPackage() {
  }

  public static Gson getGson() {
    return gson;
  }

  public void setType(final HeliumType type) {
    this.type = type;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public void setDescription(final String description) {
    this.description = description;
  }

  public void setArtifact(final String artifact) {
    this.artifact = artifact;
  }

  public void setClassName(final String className) {
    this.className = className;
  }

  public void setResources(final String[][] resources) {
    this.resources = resources;
  }

  public void setLicense(final String license) {
    this.license = license;
  }

  public void setIcon(final String icon) {
    this.icon = icon;
  }

  public String getPublished() {
    return published;
  }

  public void setPublished(final String published) {
    this.published = published;
  }

  public void setGroupId(final String groupId) {
    this.groupId = groupId;
  }

  public void setArtifactId(final String artifactId) {
    this.artifactId = artifactId;
  }

  public SpellPackageInfo getSpell() {
    return spell;
  }

  public void setSpell(final SpellPackageInfo spell) {
    this.spell = spell;
  }

  public void setConfig(final Map<String, Object> config) {
    this.config = config;
  }
}

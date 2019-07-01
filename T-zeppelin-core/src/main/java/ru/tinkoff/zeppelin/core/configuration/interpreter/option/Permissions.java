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
package ru.tinkoff.zeppelin.core.configuration.interpreter.option;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nonnull;

/**
 * Option 'permission' on interpreter configuration page.
 */
public class Permissions implements Serializable {

  @Nonnull
  private final List<String> owners;

  private boolean isEnabled;

  public Permissions() {
    this.owners = new ArrayList<>();
    this.isEnabled = false;
  }

  public Permissions(@Nonnull final List<String> owners, final boolean isEnabled) {
    this.owners = owners;
    this.isEnabled = isEnabled;
  }

  //TODO(egorklimov): conf.isUsernameForceLowerCase()??
  // было:
  //    if (null != owners && conf.isUsernameForceLowerCase()) {
  //      List<String> lowerCaseUsers = new ArrayList<String>();
  //      for (String owner : owners) {
  //        lowerCaseUsers.add(owner.toLowerCase());
  //      }
  //      return lowerCaseUsers;
  //    }
  //    return owners;
  @Nonnull
  public List<String> getOwners() {
    return owners;
  }

  public boolean isEnabled() {
    return isEnabled;
  }

  public void setEnabled(final boolean enabled) {
    isEnabled = enabled;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("owners: " + owners)
        .add("isEnabled: " + isEnabled)
        .toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof Permissions)) return false;
    final Permissions that = (Permissions) o;
    return isEnabled == that.isEnabled &&
        owners.equals(that.owners);
  }

  @Override
  public int hashCode() {
    return Objects.hash(owners, isEnabled);
  }
}

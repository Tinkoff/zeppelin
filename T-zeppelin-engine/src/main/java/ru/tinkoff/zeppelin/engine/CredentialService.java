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

import java.security.spec.KeySpec;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.Credential;
import ru.tinkoff.zeppelin.storage.CredentialsDAO;


/**
 * Service for operations on credentials
 *
 * @author Egor Klimov
 * @version 1.1
 * @since 1.1
 */
@Component
public class CredentialService {


  private final CredentialsDAO credentialsDAO;
  private final List<Credential> cache = new ArrayList<>();

  public CredentialService(final CredentialsDAO credentialsDAO) {
    this.credentialsDAO = credentialsDAO;
  }

  @PostConstruct
  private void init() {
    invalidateCache();
  }

  @Nonnull
  public Set<Credential> getUserOwnedCredentials(@Nonnull final String username,
                                                 final Set<String> groups,
                                                 final boolean isAdmin) {
    final Set<String> userAndGroups = new HashSet<>();
    userAndGroups.add(username);
    userAndGroups.addAll(groups);

    return getCache()
            .stream()
            .filter(c -> isAdmin || new HashSet<>(userAndGroups).removeAll(c.getOwners()))
            .collect(Collectors.toSet());
  }

  @Nonnull
  public Set<Credential> getUserReadableCredentials(@Nonnull final String username,
                                                    final Set<String> groups,
                                                    final boolean isAdmin) {
    final Set<String> userAndGroups = new HashSet<>();
    userAndGroups.add(username);
    userAndGroups.addAll(groups);

    return getCache()
            .stream()
            .filter(c -> isAdmin || new HashSet<>(userAndGroups).removeAll(c.getReaders()))
            .collect(Collectors.toSet());
  }

  @Nullable
  public Credential getCredentialAsReader(final long id,
                                          @Nonnull final String username,
                                          final Set<String> groups,
                                          final boolean isAdmin) {

    final Set<String> userAndGroups = new HashSet<>();
    userAndGroups.add(username);
    userAndGroups.addAll(groups);

    final Credential credential = getCache().stream().filter(c -> c.getId() == id).findFirst().orElse(null);
    if (credential == null) {
      return null;
    }

    if (isAdmin || new HashSet<>(userAndGroups).removeAll(credential.getReaders())) {
      return credential;
    } else {
      return null;
    }
  }

  @Nullable
  public Credential getCredentialAsOwner(final long id,
                                         @Nonnull final String username,
                                         final Set<String> groups,
                                         final boolean isAdmin) {

    final Set<String> userAndGroups = new HashSet<>();
    userAndGroups.add(username);
    userAndGroups.addAll(groups);

    final Credential credential = getCache().stream().filter(c -> c.getId() == id).findFirst().orElse(null);
    if (credential == null) {
      return null;
    }

    if (isAdmin || new HashSet<>(userAndGroups).removeAll(credential.getOwners())) {
      return credential;
    } else {
      return null;
    }
  }

  @Nonnull
  public Credential persistCredential(@Nonnull final Credential credential) {
    final Credential result = credentialsDAO.persist(credential);
    invalidateCache();
    return result;
  }

  @Nonnull
  public Credential updateCredential(@Nonnull final Credential credential) {
    final Credential result = credentialsDAO.update(credential);
    invalidateCache();
    return result;
  }

  public void deleteCredential(final long credentialId) {
    credentialsDAO.remove(credentialId);
    invalidateCache();
  }

  public void invalidateCache() {
    synchronized (cache) {
      cache.clear();
      cache.addAll(credentialsDAO.getAllCredentials());
    }
  }

  public List<Credential> getCache() {
    synchronized (cache) {
     return new ArrayList<>(cache);
    }
  }
}

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
package org.apache.zeppelin.rest;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.core.Credential;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.engine.CredentialService;


@RestController
@RequestMapping("/api/credential")
public class CredentialsRestApi {

  private static final String SECURE_VALUE = "*******";

  private final CredentialService credentialService;

  @Autowired
  public CredentialsRestApi(final CredentialService credentialService) {
    this.credentialService = credentialService;
  }

  @PutMapping(produces = "application/json")
  public ResponseEntity putCredentials(@RequestBody final String message) {
    try {
      final Credential credential = new Gson().fromJson(message, Credential.class);

      credentialService.persistCredential(credential);
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
  }


  public static class UserCredentialDTO {
    private Credential credentialInfo;
    private boolean isOwner;

    UserCredentialDTO(final Credential credentialInfo,
                      final boolean isOwner) {
      this.credentialInfo = credentialInfo;
      this.isOwner = isOwner;
    }
  }

  @GetMapping(produces = "application/json")
  public ResponseEntity getCredentials() {
    try {
      final AuthenticationInfo auth = AuthorizationService.getAuthenticationInfo();

      final List<UserCredentialDTO> credentials = new ArrayList<>();

      final Set<Credential> readable = credentialService.getUserReadableCredentials(auth.getUser(), auth.getRoles(), isAdmin());
      final Set<Credential> owned = credentialService.getUserOwnedCredentials(auth.getUser(), auth.getRoles(), isAdmin());

      readable.removeAll(owned);
      readable.forEach(c -> credentials.add(new UserCredentialDTO(c, false)));
      owned.forEach(c -> credentials.add(new UserCredentialDTO(c, true)));
      // TODO: FIX
      credentials.stream()
              .filter(dto -> dto.credentialInfo.getSecure())
              .forEach(dto -> dto.credentialInfo = new Credential(dto.credentialInfo.getId(),
                      dto.credentialInfo.getKey(),
                      SECURE_VALUE,
                      dto.credentialInfo.getSecure(),
                      dto.credentialInfo.getDescription(),
                      dto.credentialInfo.getReaders(),
                      dto.credentialInfo.getOwners())
              );

      return new JsonResponse(HttpStatus.OK, "", credentials).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
  }


  @PostMapping(produces = "application/json")
  public ResponseEntity updateCredential(@RequestBody final String message) {
    try {
      final AuthenticationInfo auth = AuthorizationService.getAuthenticationInfo();

      final Credential credential = new Gson().fromJson(message, Credential.class);

      final Credential original = credentialService.getCredentialAsOwner(
              credential.getId(),
              auth.getUser(),
              auth.getRoles(),
              isAdmin()
      );

      if (original == null) {
        return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Credential not found or insufficient privileges").build();
      }

      final String originalValue = original.getSecure()
              ? SECURE_VALUE
              : original.getValue();

      final Credential updated = new Credential(
              original.getId(),
              original.getKey(),
              credential.getValue().equals(originalValue) && (credential.getSecure() || !original.getSecure())
                      ? original.getValue()
                      : credential.getValue(),
              credential.getSecure(),
              credential.getDescription(),
              credential.getReaders(),
              credential.getOwners()
      );
      credentialService.updateCredential(updated);

      return new JsonResponse(HttpStatus.OK, "", updated).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
  }

  @DeleteMapping(value = "/{key}", produces = "application/json")
  public ResponseEntity removeCredential(@PathVariable("key") final long key) {
    try {
      final AuthenticationInfo auth = AuthorizationService.getAuthenticationInfo();

      final Credential original = credentialService.getCredentialAsOwner(
              key,
              auth.getUser(),
              auth.getRoles(),
              isAdmin()
      );

      if (original == null) {
        return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Credential not found or insufficient privileges").build();
      }

      credentialService.deleteCredential(key);
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
  }

  private boolean isAdmin() {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Set<String> userRoles = new HashSet<>();
    userRoles.add(authenticationInfo.getUser());
    userRoles.addAll(authenticationInfo.getRoles());

    final Set<String> admin = new HashSet<>();
    admin.addAll(Configuration.getAdminUsers());
    admin.addAll(Configuration.getAdminGroups());
    for (final String availableRole : userRoles) {
      if (admin.contains(availableRole)) {
        return true;
      }
    }
    return false;
  }

}

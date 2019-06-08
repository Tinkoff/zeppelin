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
import java.util.Set;
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
import ru.tinkoff.zeppelin.engine.CredentialService;


@RestController
@RequestMapping("/api/credential")
public class CredentialsRestApi {

  private final CredentialService credentialService;

  @Autowired
  public CredentialsRestApi(final CredentialService credentialService) {
    this.credentialService = credentialService;
  }

  @PutMapping(produces = "application/json")
  public ResponseEntity putCredentials(@RequestBody final String message) {
    try {
      final String user = AuthorizationService.getAuthenticationInfo().getUser();
      final Credential credential = new Gson().fromJson(message, Credential.class);
      credential.getOwners().add(user);
      credential.getReaders().add(user);

      credentialService.persistCredential(credential);
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
  }


  public static class UserCredentialDTO {
    private final Credential credentialInfo;
    private final boolean isOwner;

    UserCredentialDTO(final Credential credentialInfo,
                      final boolean isOwner) {
      this.credentialInfo = credentialInfo;
      this.isOwner = isOwner;
    }
  }

  @GetMapping(produces = "application/json")
  public ResponseEntity getCredentials() {
    try {
      final String user = AuthorizationService.getAuthenticationInfo().getUser();
      final ArrayList<UserCredentialDTO> credentials = new ArrayList<>();

      final Set<Credential> readable = credentialService.getUserReadableCredentials(user);
      final Set<Credential> owned = credentialService.getUserOwnedCredentials(user);

      readable.removeAll(owned);
      readable.forEach(c -> credentials.add(new UserCredentialDTO(c, false)));
      owned.forEach(c -> credentials.add(new UserCredentialDTO(c, true)));

      return new JsonResponse(HttpStatus.OK, "", credentials).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
  }


  @PostMapping(produces = "application/json")
  public ResponseEntity updateCredential(@RequestBody final String message) {
    try {
      final Credential credential = new Gson().fromJson(message, Credential.class);

      final String user = AuthorizationService.getAuthenticationInfo().getUser();
      final Credential original = credentialService.getCredential(credential.getId());

      if (original == null) {
        return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Credential not found").build();
      } else if (!original.getOwners().contains(user)) {
        return new JsonResponse(HttpStatus.BAD_REQUEST, "You must be an owner of the credential").build();
      }

      credentialService.updateCredential(credential);
      return new JsonResponse(HttpStatus.OK, "", credential).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
  }

  @DeleteMapping(value = "/{key}", produces = "application/json")
  public ResponseEntity removeCredential(@PathVariable("key") final String key) {
    try {
      credentialService.deleteCredential(key);
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
  }
}

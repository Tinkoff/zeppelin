package org.apache.zeppelin.rest;

import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.storage.BaseConfigurationDAO;

import java.util.HashSet;
import java.util.Set;

@RestController
@RequestMapping("/api/configurations")
public class BaseConfigurationRestApi {
  private final BaseConfigurationDAO baseConfigurationDAO;

  @Autowired
  public BaseConfigurationRestApi(final BaseConfigurationDAO baseConfigurationDAO){
    this.baseConfigurationDAO = baseConfigurationDAO;
  }

  @GetMapping(value = "/all", produces = "application/json")
  public ResponseEntity getConfiguration() {
    if(!isAdmin()) {
      return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
    }

    return new JsonResponse(HttpStatus.OK, baseConfigurationDAO.get()).build();
  }

  @PostMapping(value = "/set", produces = "application/json")
  public ResponseEntity setConfigurationParam(@RequestParam("name") final String name,
                                              @RequestParam("value") final String value) {

    if(!isAdmin()) {
      return new JsonResponse(HttpStatus.FORBIDDEN, "FORBIDDEN").build();
    }

    if (baseConfigurationDAO.getByName(name) != null){
      baseConfigurationDAO.update(name,value);
      return new JsonResponse(HttpStatus.OK, "").build();
    }
    return new JsonResponse(HttpStatus.BAD_REQUEST, "Unsupported configuration parameter.").build();
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

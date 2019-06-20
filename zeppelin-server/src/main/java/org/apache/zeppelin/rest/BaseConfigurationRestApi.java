package org.apache.zeppelin.rest;

import org.apache.zeppelin.rest.message.JsonResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.tinkoff.zeppelin.storage.BaseConfigurationDAO;

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
    return new JsonResponse(HttpStatus.OK, baseConfigurationDAO.get()).build();
  }

  @PostMapping(value = "/set", produces = "application/json")
  public ResponseEntity setConfigurationParam(@RequestParam("name") final String name,
                                              @RequestParam("value") final String value){
    if (baseConfigurationDAO.getByName(name) != null){
      baseConfigurationDAO.update(name,value);
      return new JsonResponse(HttpStatus.OK, "").build();
    }
    return new JsonResponse(HttpStatus.BAD_REQUEST, "Unsupported configuration parameter.").build();
  }
}

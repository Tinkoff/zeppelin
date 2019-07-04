package ru.tinkoff.zeppelin.commons.jdbc.analyzer;

import java.util.List;

public interface ResourceDAO {
  List<ResourceDTO> getAllResourceInfo();
}

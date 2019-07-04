package ru.tinkoff.zeppelin.commons.jdbc.analyzer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import javax.sql.DataSource;


/**
 * Class representing JDBCInterepreter analyzer:
 *
 */
public abstract class Analyzer {

  private final AtomicBoolean isUpdating = new AtomicBoolean(false);
  private final ConcurrentHashMap<Long, ResourceDTO> resourceStatistics = new ConcurrentHashMap<>();

  protected ResourceDAO resourceDAO;

  /**
   * Returns DataSource which could be used in DAO.
   *
   * @param configuration
   * @return
   */
  protected abstract DataSource open(@Nonnull Map<String, String> configuration);

  /**
   *
   */
  protected void updateResourceStatistics() {
    isUpdating.set(true);
    resourceStatistics.clear();
    for (final ResourceDTO resource : resourceDAO.getAllResourceInfo()) {
      resourceStatistics.put(resource.getResourceId(), resource);
    }
    isUpdating.set(false);
  }

  /**
   *
   * @param resourceId
   * @return
   */
  public ResourceDTO getResourceQueueActivity(final long resourceId) {
    while (isUpdating.get()) {
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (final InterruptedException e) {
        // skip
      }
    }
    return resourceStatistics.get(resourceId);
  }

  /**
   *
   * @param msg
   * @return
   */
  public static String logMessage(final String msg) {
    return String.format(
        "%s INFO: %s%s",
        new SimpleDateFormat("HH:mm:ss").format(new Date()),
        msg,
        System.lineSeparator()
    );
  }
}

package ru.tinkoff.zeppelin.interpreter.jdbc.gp.analyzer;

import org.springframework.jdbc.datasource.DriverManagerDataSource;
import ru.tinkoff.zeppelin.commons.jdbc.analyzer.Analyzer;
import ru.tinkoff.zeppelin.commons.jdbc.utils.JDBCInstallation;
import ru.tinkoff.zeppelin.interpreter.Context;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GPAnalyzer extends Analyzer {

  private static final String CONNECTION_USER_KEY = "connection.user";
  private static final String CONNECTION_URL_KEY = "connection.url";
  private static final String CONNECTION_PASSWORD_KEY = "connection.password";

  private static final String DRIVER_CLASS_NAME_KEY = "driver.className";
  private static final String DRIVER_ARTIFACT_KEY = "driver.artifact";
  private static final String DRIVER_ARTIFACT_DEPENDENCY = "driver.artifact.dependency";
  private static final String DRIVER_MAVEN_REPO_KEY = "driver.maven.repository.url";


  public GPAnalyzer(@Nonnull final Context context) {
    final DataSource dataSource = open(context);
    this.resourceDAO = new GPResourceGroupActivityDAO(dataSource);

    Executors.newSingleThreadScheduledExecutor()
        .scheduleWithFixedDelay(this::updateResourceStatistics, 1, 1, TimeUnit.MINUTES);
  }

  /**
   * Installs driver if needed and opens the database connection.
   *
   * @param context interpreter context.
   */
  @Override
  protected DataSource open(@Nonnull final Context context) {
    final String className = context.getConfiguration().get(DRIVER_CLASS_NAME_KEY);
    final String artifact = context.getConfiguration().get(DRIVER_ARTIFACT_KEY);
    final String artifactDependencies = context.getConfiguration().get(DRIVER_ARTIFACT_DEPENDENCY);
    final String user = context.getConfiguration().get(CONNECTION_USER_KEY);
    final String dbUrl = context.getConfiguration().get(CONNECTION_URL_KEY);
    final String password = context.getConfiguration().get(CONNECTION_PASSWORD_KEY);

    if (className != null
        && artifact != null
        && user != null
        && dbUrl != null
        && password != null) {

      final String repositpryURL = context.getConfiguration().getOrDefault(
          DRIVER_MAVEN_REPO_KEY,
          "http://repo1.maven.org/maven2/"
      );
      final List<String> dependencies = new ArrayList<>();
      if (artifactDependencies != null) {
        dependencies.addAll(Arrays.asList(artifactDependencies.split(";")));
      }
      JDBCInstallation.installDriver(artifact, dependencies, repositpryURL);
      return new DriverManagerDataSource(dbUrl, user, password);
    }
    throw new RuntimeException();
  }
}
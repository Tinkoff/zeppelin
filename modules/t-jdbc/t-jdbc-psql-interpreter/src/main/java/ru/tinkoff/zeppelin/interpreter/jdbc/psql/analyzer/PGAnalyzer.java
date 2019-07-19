package ru.tinkoff.zeppelin.interpreter.jdbc.psql.analyzer;

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

public class PGAnalyzer extends Analyzer {

  private static final String CONNECTION_USER_KEY = "remoteConnection.user";
  private static final String CONNECTION_URL_KEY = "remoteConnection.url";
  private static final String CONNECTION_PASSWORD_KEY = "remoteConnection.password";

  private static final String DRIVER_CLASS_NAME_KEY = "driver.className";
  private static final String DRIVER_ARTIFACT_KEY = "driver.artifact";
  private static final String DRIVER_ARTIFACT_DEPENDENCY = "driver.artifact.dependency";
  private static final String DRIVER_MAVEN_REPO_KEY = "driver.maven.repository.url";

  public PGAnalyzer(@Nonnull final Context context) {
    final DataSource dataSource = open(context);
    this.resourceDAO = new PGResourceGroupActivityDAO(dataSource);

    Executors.newSingleThreadScheduledExecutor()
        .scheduleWithFixedDelay(this::updateResourceStatistics, 0, 20, TimeUnit.SECONDS);
  }

  /**
   * Installs driver if needed and opens the database remoteConnection.
   *
   * @param context interpreter configuration.
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
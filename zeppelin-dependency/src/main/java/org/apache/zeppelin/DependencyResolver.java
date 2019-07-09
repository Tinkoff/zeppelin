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

package org.apache.zeppelin;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.repository.internal.MavenRepositorySystemSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sonatype.aether.RepositoryException;
import org.sonatype.aether.RepositorySystem;
import org.sonatype.aether.artifact.Artifact;
import org.sonatype.aether.collection.CollectRequest;
import org.sonatype.aether.graph.Dependency;
import org.sonatype.aether.graph.DependencyFilter;
import org.sonatype.aether.repository.LocalRepository;
import org.sonatype.aether.repository.LocalRepositoryManager;
import org.sonatype.aether.repository.RemoteRepository;
import org.sonatype.aether.resolution.ArtifactResult;
import org.sonatype.aether.resolution.DependencyRequest;
import org.sonatype.aether.resolution.DependencyResolutionException;
import org.sonatype.aether.util.artifact.DefaultArtifact;
import org.sonatype.aether.util.artifact.JavaScopes;
import org.sonatype.aether.util.filter.DependencyFilterUtils;
import org.sonatype.aether.util.filter.PatternExclusionsDependencyFilter;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;


/**
 * Deps resolver.
 * Add new dependencies from mvn repository (at runtime) to Zeppelin.
 */
public final class DependencyResolver {

  private final static Logger logger = LoggerFactory.getLogger(DependencyResolver.class);

  private DependencyResolver() {
  }

  public static List<File> load(final List<String> remoteRepositories, final String artifact, final File destPath) throws RepositoryException, IOException {
    final RepositorySystem system = RepositorySystemFactory.newRepositorySystem();
    final LocalRepository localRepo = new LocalRepository(System.getProperty("user.home") + "/.m2/repository");
    //final LocalRepository localRepo = new LocalRepository(new File(destPath.getAbsolutePath(), "M2"));
    final LocalRepositoryManager manager = system.newLocalRepositoryManager(localRepo);

    final MavenRepositorySystemSession session = new MavenRepositorySystemSession();
    session.setLocalRepositoryManager(manager);

    final List<RemoteRepository> repos = new LinkedList<>();
    for (final String repository : remoteRepositories) {
      repos.add(new RemoteRepository("central", "default", repository));
    }

    List<File> libs = new LinkedList<>();

    if (StringUtils.isNotBlank(artifact)) {
      libs = loadFromMvn(artifact, repos, session, system);

      for (final File srcFile : libs) {
        final File destFile = new File(destPath, srcFile.getName());
        if (!destFile.exists() || !FileUtils.contentEquals(srcFile, destFile)) {
          FileUtils.copyFile(srcFile, destFile);
          logger.debug("copy {} to {}", srcFile.getAbsolutePath(), destPath);
        }
      }
    }
    return libs;
  }

  private static List<File> loadFromMvn(final String artifactName,
                                        final List<RemoteRepository> repos,
                                        final MavenRepositorySystemSession session,
                                        final RepositorySystem system) throws RepositoryException {

    final List<ArtifactResult> listOfArtifact;
    final Artifact artifact = new DefaultArtifact(artifactName);
    final DependencyFilter classpathFilter = DependencyFilterUtils.classpathFilter(JavaScopes.COMPILE);
    final PatternExclusionsDependencyFilter exclusionFilter = new PatternExclusionsDependencyFilter();

    final CollectRequest collectRequest = new CollectRequest();
    collectRequest.setRoot(new Dependency(artifact, JavaScopes.COMPILE));

    for (final RemoteRepository repo : repos) {
      collectRequest.addRepository(repo);
    }

    final DependencyRequest dependencyRequest = new DependencyRequest(collectRequest,
            DependencyFilterUtils.andFilter(exclusionFilter, classpathFilter));
    try {
      listOfArtifact = system.resolveDependencies(session, dependencyRequest).getArtifactResults();
    } catch (final NullPointerException | DependencyResolutionException ex) {
      throw new RepositoryException(
              String.format("Cannot fetch dependencies for %s", artifactName), ex);
    }

    final List<File> files = new LinkedList<>();
    for (final ArtifactResult artifactResult : listOfArtifact) {
      files.add(artifactResult.getArtifact().getFile());
      logger.debug("load {}", artifactResult.getArtifact().getFile().getAbsolutePath());
    }

    return files;
  }
}

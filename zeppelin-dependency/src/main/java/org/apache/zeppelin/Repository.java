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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.sonatype.aether.repository.Authentication;
import org.sonatype.aether.repository.Proxy;

import java.util.Objects;

/**
 * A repository hosting module sources.
 *
 * @see ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleSource
 */
public class Repository {

  /**
   * Types of proxy protocols.
   */
  public enum ProxyProtocol {
    HTTP,
    HTTPS
  }

  private boolean snapshot;

  @Nonnull
  private String id;

  @Nonnull
  private String url;

  @Nullable
  private String username;

  @Nullable
  private String password;

  @Nullable
  private ProxyProtocol proxyProtocol;

  @Nullable
  private String proxyHost;

  @Nullable
  private Integer proxyPort;

  @Nullable
  private String proxyLogin;

  @Nullable
  private String proxyPassword;

  /**
   * Creates a new repository with the specified properties.
   *
   * @param snapshot @param snapshot {@code true} to use the snapshot policy, {@code false} to use the release policy.
   * @param id The id (case-sensitive), never {@code null}.
   * @param url The repo url, never {@code null}.
   * @param username The username (case-sensitive), may be {@code null}.
   * @param password The password (case-sensitive), may be {@code null}.
   * @param proxyProtocol The proxy protocol, may be {@code null}.
   * @param proxyHost The proxy host, may be {@code null}.
   * @param proxyPort The proxy port, may be {@code null}.
   * @param proxyLogin The proxy login (case-sensitive), may be {@code null}.
   * @param proxyPassword The proxy password (case-sensitive), may be {@code null}.
   */
  public Repository(
      final boolean snapshot,
      @Nonnull final String id,
      @Nonnull final String url,
      @Nullable final String username,
      @Nullable final String password,
      @Nullable final ProxyProtocol proxyProtocol,
      @Nullable final String proxyHost,
      @Nullable final Integer proxyPort,
      @Nullable final String proxyLogin,
      @Nullable final String proxyPassword) {
    Preconditions.checkNotNull(id);
    Preconditions.checkNotNull(url);
    Preconditions.checkArgument(isValidPort(proxyPort), "Incorrect proxy port: %s", proxyPort);

    this.snapshot = snapshot;
    this.id = id;
    this.url = url;
    this.username = username;
    this.password = password;
    this.proxyProtocol = proxyProtocol;
    this.proxyHost = proxyHost;
    this.proxyPort = proxyPort;
    this.proxyLogin = proxyLogin;
    this.proxyPassword = proxyPassword;
  }


  /**
   * Creates authentication to use for accessing a protected resource.
   * <em>Note:</em> Instances of this class are immutable and the exposed mutators return new
   * objects rather than changing the current instance.
   *
   * @return The authentication, never {@code null}. But username and password may be {@code null}.
   */
  @Nonnull
  public Authentication getAuthentication() {
    return new Authentication(this.username, this.password);
  }


  /**
   * Creates a proxy to use for connections to a repository.
   * <em>Note:</em> Instances of this class are immutable and the exposed
   * mutators return new objects rather than changing the current instance.
   *
   * @return The proxy or {@code null} if proxyHost, proxyPort, proxyProtocol are not set.
   */
  @Nullable
  public Proxy getProxy() {
    if (isNotBlank(proxyHost) && proxyPort != null && proxyProtocol != null) {
      if (isNotBlank(proxyLogin)) {
        return new Proxy(proxyProtocol.name().toLowerCase(), proxyHost, proxyPort,
            new Authentication(proxyLogin, proxyPassword));
      } else {
        return new Proxy(proxyProtocol.name().toLowerCase(), proxyHost, proxyPort, null);
      }
    }
    return null;
  }

  /**
   * Gets the snapshot flag.
   *
   * @return The snapshot {@code true} to use the snapshot policy, {@code false} to use the release policy.
   */
  public boolean isSnapshot() {
    return snapshot;
  }


  /**
   * Sets the snapshot flag.
   *
   * {@code true} to use the snapshot policy, {@code false} to use the release policy.
   */
  public void setSnapshot(final boolean snapshot) {
    this.snapshot = snapshot;
  }

  /**
   * Gets repository id in which the it is available.
   *
   * @return Repository id, never {@code null}
   */
  @Nonnull
  public String getId() {
    Preconditions.checkNotNull(id);
    return id;
  }

  /**
   * Sets repository id in which it is available.
   *
   * @param id Repository id, never {@code null}
   */
  public void setId(@Nonnull final String id) {
    Preconditions.checkNotNull(id);
    this.id = id;
  }

  /**
   * Gets repository url.
   *
   * @return Repository url, never {@code null}.
   */
  @Nonnull
  public String getUrl() {
    Preconditions.checkNotNull(url);
    return url;
  }

  /**
   * Sets repository url.
   *
   * @param url, Repository url, never {@code null}.
   */
  public void setUrl(@Nonnull final String url) {
    Preconditions.checkNotNull(url);
    this.url = url;
  }

  /**
   * Gets the username which used to authenticate repository.
   *
   * @return username, may be {@code null}.
   * @see Repository#getAuthentication()
   */
  @Nullable
  public String getUsername() {
    return username;
  }

  /**
   * Sets the username which used to authenticate repository.
   *
   * @param username, username, may be {@code null}.
   * @see Repository#getAuthentication()
   */
  public void setUsername(@Nonnull final String username) {
    Preconditions.checkNotNull(username);
    this.username = username;
  }

  /**
   * Gets the password which used to authenticate repository.
   *
   * @return password, password, may be {@code null}.
   * @see Repository#getAuthentication()
   */
  @Nullable
  public String getPassword() {
    return password;
  }

  /**
   * Sets the password which used to authenticate repository.
   *
   * @param  password, password, may be {@code null}.
   * @see Repository#getAuthentication()
   */
  public void setPassword(@Nullable final String password) {
    this.password = password;
  }

  /**
   * Gets the proxy protocol.
   *
   * @return protocol type, may be {@code null}.
   * @see ProxyProtocol
   * @see Repository#getProxy()
   */
  @Nullable
  public ProxyProtocol getProxyProtocol() {
    return proxyProtocol;
  }

  /**
   * Sets the proxy protocol.
   *
   * @param proxyProtocol, protocol may be {@code null}.
   * @see ProxyProtocol
   * @see Repository#getProxy()
   */
  public void setProxyProtocol(@Nullable final ProxyProtocol proxyProtocol) {
    this.proxyProtocol = proxyProtocol;
  }

  /**
   * Gets the proxy host.
   *
   * @return Proxy Host, may be {@code null}.
   * @see Repository#getProxy()
   */
  @Nullable
  public String getProxyHost() {
    return proxyHost;
  }

  /**
   * Sets the proxy host.
   *
   * @param proxyHost, Proxy Host, may be {@code null}.
   * @see Repository#getProxy()
   */
  public void setProxyHost(@Nullable final String proxyHost) {
    this.proxyHost = proxyHost;
  }

  /**
   * Gets the proxy port.
   *
   * @return Proxy port, may be {@code null}.
   * @see Repository#getProxy()
   */
  @Nullable
  public Integer getProxyPort() {
    Preconditions.checkState(isValidPort(proxyPort), "Wrong proxy port - {}", proxyPort);
    return proxyPort;
  }

  /**
   * Sets the proxy port.
   *
   * @param proxyPort, Proxy port, may be {@code null}.
   * @see Repository#getProxy()
   */
  public void setProxyPort(@Nullable final Integer proxyPort) {
    Preconditions.checkArgument(isValidPort(proxyPort), "Wrong proxy port passed - {}", proxyPort);
    this.proxyPort = proxyPort;
  }

  /**
   * Gets the proxy login.
   *
   * @return Proxy login, may be {@code null}.
   * @see Repository#getProxy()
   */
  @Nullable
  public String getProxyLogin() {
    return proxyLogin;
  }

  /**
   * Sets the proxy login.
   *
   * @param proxyLogin, Proxy login, may be {@code null}.
   * @see Repository#getProxy()
   */
  public void setProxyLogin(@Nullable final String proxyLogin) {
    this.proxyLogin = proxyLogin;
  }

  /**
   * Gets the proxy password.
   *
   * @return Proxy password, may be {@code null}.
   * @see Repository#getProxy()
   */
  @Nullable
  public String getProxyPassword() {
    return proxyPassword;
  }

  /**
   * Sets the proxy password.
   *
   * @param proxyPassword, Proxy password, may be {@code null}.
   * @see Repository#getProxy()
   */
  public void setProxyPassword(@Nullable final String proxyPassword) {
    this.proxyPassword = proxyPassword;
  }

  @Nonnull
  public String toJson() {
    Preconditions.checkNotNull(this.url);
    Preconditions.checkNotNull(this.id);
    Preconditions.checkState(isValidPort(proxyPort), "Wrong proxy port - {}", proxyPort);
    //TODO(egorklimov): check urls using regexp
    return new Gson().toJson(this);
  }

  @Nonnull
  public static Repository fromJson(@Nonnull final String json) {
    Preconditions.checkNotNull(json);
    final Repository repository = new Gson().fromJson(json, Repository.class);

    // Gson#fromJson is nullable.
    Preconditions.checkNotNull(repository);
    Preconditions.checkNotNull(repository.id);
    Preconditions.checkNotNull(repository.url);
    Preconditions.checkState(isValidPort(repository.proxyPort), "Wrong proxy port - {}", repository.proxyPort);
    //TODO(egorklimov): check urls using regexp
    return repository;
  }

  /**
   * Port number validation:
   *    null or 0: proxy port is not set.
   *    [1; 2^16): available ports.
   *
   * @param port, port number, may be {@code null}.
   * @return {@code true} if port is valid, {@code false} otherwise.
   */
  private static boolean isValidPort(@Nullable final Integer port) {
    return port == null || port >= 0 && port < Math.pow(2, 16);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof Repository)) return false;
    final Repository that = (Repository) o;
    return snapshot == that.snapshot &&
        id.equals(that.id) &&
        url.equals(that.url) &&
        Objects.equals(username, that.username) &&
        Objects.equals(password, that.password) &&
        proxyProtocol == that.proxyProtocol &&
        Objects.equals(proxyHost, that.proxyHost) &&
        Objects.equals(proxyPort, that.proxyPort) &&
        Objects.equals(proxyLogin, that.proxyLogin) &&
        Objects.equals(proxyPassword, that.proxyPassword);
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshot, id, url, username, password, proxyProtocol, proxyHost, proxyPort, proxyLogin, proxyPassword);
  }
}

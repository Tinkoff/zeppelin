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

package org.apache.zeppelin.realm;


import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.session.Session;
import org.apache.shiro.session.mgt.DefaultSessionManager;
import org.apache.shiro.util.ThreadContext;
import org.apache.shiro.web.mgt.DefaultWebSecurityManager;

public class AuthorizationService {

  public static String ANONYMOUS = "anonymous";

  private static final ThreadLocal<String> principalThreadLocal = new ThreadLocal<>();
  private static final Map<String, AuthenticationInfo> cache = new ConcurrentHashMap<>();

  public static void setThreadLocalPrincipal(final String principal) {
    principalThreadLocal.set(principal);
  }

  public static AuthenticationInfo getAuthenticationInfo() {
    // get security manager
    final DefaultWebSecurityManager securityManager = (DefaultWebSecurityManager)SecurityUtils.getSecurityManager();
    if(securityManager == null) {
      final AuthenticationInfo authenticationInfo = new AuthenticationInfo(ANONYMOUS, new HashSet<>());
      cache.put(ANONYMOUS, authenticationInfo);
      return authenticationInfo;
    }

    final Collection<Realm> realms = securityManager.getRealms();
    if(realms == null || realms.isEmpty()) {
      final AuthenticationInfo authenticationInfo = new AuthenticationInfo(ANONYMOUS, new HashSet<>());
      cache.put(ANONYMOUS, authenticationInfo);
      return authenticationInfo;
    }

    String principal = null;
    for (final Realm realm : realms) {
      final ShiroSecurityService securityService = (ShiroSecurityService)realm;
      if(securityService.getPrincipal() != null) {
        principal = securityService.getPrincipal();
        break;
      }
    }
    if(principal == null) {
      principal = principalThreadLocal.get();
    }

    if(principal == null || ANONYMOUS.equals(principal)) {
      final AuthenticationInfo authenticationInfo = new AuthenticationInfo(ANONYMOUS, new HashSet<>());
      cache.put(ANONYMOUS, authenticationInfo);
      return authenticationInfo;
    }

    if (cache.containsKey(principal) && !cache.get(principal).getRoles().isEmpty()) {
      return cache.get(principal);
    } else {
      // load groups
      final Set<String> principalRoles = new HashSet<>();
      for (final Realm realm : realms) {
        final ShiroSecurityService securityService = (ShiroSecurityService)realm;
          principalRoles.addAll(securityService.getAssociatedRoles(principal));
      }
      final AuthenticationInfo authenticationInfo
              = new AuthenticationInfo(principal, principalRoles);

      cache.put(principal, authenticationInfo);
      return authenticationInfo;
    }
  }

  public static AuthenticationInfo getAuthenticationInfo(final String principal) {

    final DefaultWebSecurityManager securityManager = (DefaultWebSecurityManager)SecurityUtils.getSecurityManager();
    if(securityManager == null) {
     throw new UnsupportedOperationException("Can not load user groups. securityManager not initialized");
    }

    final Collection<Realm> realms = securityManager.getRealms();
    if(realms == null || realms.isEmpty()) {
      throw new UnsupportedOperationException("Can not load user groups. realms not initialized");
    }

    if (cache.containsKey(principal) && !cache.get(principal).getRoles().isEmpty()) {
      return cache.get(principal);
    } else {
      // load groups
      final Set<String> principalRoles = new HashSet<>();
      for (final Realm realm : realms) {
        final ShiroSecurityService securityService = (ShiroSecurityService)realm;
        principalRoles.addAll(securityService.getAssociatedRoles(principal));
      }
      return new AuthenticationInfo(principal, principalRoles);
    }
  }

  public static void drop(final String principal) {
    cache.remove(principal);

    final Serializable userSession = SecurityUtils.getSubject().getSession(false).getId();
    DefaultSecurityManager securityManager = (DefaultSecurityManager) SecurityUtils.getSecurityManager();
    DefaultSessionManager sessionManager = (DefaultSessionManager) securityManager.getSessionManager();
    Collection<Session> activeSessions = sessionManager.getSessionDAO().getActiveSessions();
    for (Session session: activeSessions){
      if (userSession.equals(session.getId())) {
        session.stop();
      }
    }
    ThreadContext.remove(ThreadContext.SUBJECT_KEY);
  }
}

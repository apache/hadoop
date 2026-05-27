/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.http;

import java.security.Principal;
import javax.security.auth.Subject;
import javax.servlet.ServletRequest;

import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.UserIdentity;

/**
 * Bridges Hadoop's filter-based authentication to Jetty's native
 * {@link Authentication} state on the base {@link Request}.
 *
 * <p>Hadoop authenticates with servlet filters
 * ({@link org.apache.hadoop.security.authentication.server.AuthenticationFilter}
 * and friends). Those filters wrap the {@code HttpServletRequest} so
 * downstream filters and servlets see the authenticated user via the
 * standard servlet API. The wrapper, however, only flows through the filter
 * chain — Jetty observers that run outside the chain
 * ({@code RequestLogHandler}, {@code StatisticsHandler}, async dispatch,
 * JMX) all see the base {@link Request}, where {@code getAuthentication()}
 * stays as {@code NOT_CHECKED} forever. As a result, the {@code %u} token
 * in Jetty's default access log is logged as {@code -}.
 *
 * <p>After {@link #attach}, Jetty's standard API resolves the user from the
 * base Request, so RequestLog (and anything else that reads from the Jetty
 * layer) sees it without any Hadoop-specific knowledge.
 */
public final class HadoopJettyAuthentication {

  private static final String AUTH_METHOD = "HADOOP";

  private HadoopJettyAuthentication() {
  }

  /**
   * Attach {@code userName} to the base Jetty {@link Request} as the
   * authenticated user. No-op if either argument is {@code null}, or if the
   * underlying request is not a Jetty {@link Request} (e.g., during tests
   * with mock requests).
   */
  public static void attach(ServletRequest request, String userName) {
    if (request == null || userName == null) {
      return;
    }
    Request baseRequest = Request.getBaseRequest(request);
    if (baseRequest == null) {
      return;
    }
    baseRequest.setAuthentication(buildAuthentication(userName));
  }

  /** Package-private for testing. */
  static Authentication.User buildAuthentication(String userName) {
    Principal principal = new HadoopPrincipal(userName);
    Subject subject = new Subject();
    subject.getPrincipals().add(principal);
    subject.setReadOnly();

    UserIdentity identity = new HadoopUserIdentity(subject, principal);
    return new HadoopAuthenticatedUser(identity);
  }

  private static final class HadoopPrincipal implements Principal {
    private final String name;

    HadoopPrincipal(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  private static final class HadoopUserIdentity implements UserIdentity {
    private final Subject subject;
    private final Principal principal;

    HadoopUserIdentity(Subject subject, Principal principal) {
      this.subject = subject;
      this.principal = principal;
    }

    @Override
    public Subject getSubject() {
      return subject;
    }

    @Override
    public Principal getUserPrincipal() {
      return principal;
    }

    @Override
    public boolean isUserInRole(String role, Scope scope) {
      return false;
    }
  }

  private static final class HadoopAuthenticatedUser
      implements Authentication.User {
    private final UserIdentity identity;

    HadoopAuthenticatedUser(UserIdentity identity) {
      this.identity = identity;
    }

    @Override
    public String getAuthMethod() {
      return AUTH_METHOD;
    }

    @Override
    public UserIdentity getUserIdentity() {
      return identity;
    }

    @Override
    public boolean isUserInRole(UserIdentity.Scope scope, String role) {
      return false;
    }

    @Override
    public void logout() {
      // Programmatic logout is not supported by Hadoop filter-based auth.
    }

    @Override
    public Authentication logout(ServletRequest request) {
      return null;
    }
  }
}

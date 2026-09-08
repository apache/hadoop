/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.server;

import java.security.Principal;
import java.util.Collections;
import javax.security.auth.Subject;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes the authenticated user on the underlying Jetty {@link Request} so
 * the access log %u can resolve it. Hadoop's auth filters expose the user via
 * an {@code HttpServletRequestWrapper}, which Jetty's request log handler does
 * not see; pushing the authentication onto the base request makes the user
 * visible after the filter chain returns.
 */
public final class JettyAuthenticationHelper {
  private static final Logger LOG = LoggerFactory.getLogger(JettyAuthenticationHelper.class);

  private JettyAuthenticationHelper() {
  }

  /**
   * Publishes {@code request.getRemoteUser()} as the authenticated user on
   * the underlying Jetty request. First writer wins so that callers that
   * resolve the effective user earliest (e.g. delegation-token handler with
   * the doAs user) are not overwritten by later filter-chain hooks. No-op
   * when there is no remote user, when the request is not running on Jetty,
   * or when the base request already has an {@link Authentication.User}.
   *
   * @param request the wrapped HTTP request, after the auth filter has set
   *                the remote user
   */
  public static void publishRemoteUser(HttpServletRequest request) {
    if (request == null) {
      return;
    }
    String user = request.getRemoteUser();
    publishRemoteUser(request, user);
  }

  /**
   * Same as {@link #publishRemoteUser(HttpServletRequest)} but uses the
   * provided user name instead of {@code request.getRemoteUser()}. Use this
   * when the effective user (e.g. doAs) is not yet reflected on the request.
   *
   * @param request the HTTP request used to find the underlying Jetty request
   * @param user    the user name to publish
   */
  public static void publishRemoteUser(HttpServletRequest request, String user) {
    if (user == null || user.isEmpty()) {
      return;
    }
    Request base = Request.getBaseRequest(request);
    if (base == null) {
      return;
    }

    Authentication existing = base.getAuthentication();
    if (existing instanceof Authentication.User) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("publishRemoteUser skipped: already published existing='{}', incoming='{}'",
            ((Authentication.User) existing).getUserIdentity()
                .getUserPrincipal().getName(), user);
      }
      return;
    }
    LOG.debug("publishRemoteUser published user='{}'", user);
    base.setAuthentication(new RemoteUserAuthentication(user));
  }

  private static final class RemoteUserAuthentication
      implements Authentication.User {
    private final UserIdentity identity;

    RemoteUserAuthentication(String name) {
      Principal principal = new RemoteUserPrincipal(name);
      Subject subject = new Subject(true,
          Collections.singleton(principal),
          Collections.emptySet(),
          Collections.emptySet());
      this.identity = new RemoteUserIdentity(subject, principal);
    }

    @Override
    public String getAuthMethod() {
      return "HADOOP";
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
    }

    @Override
    public Authentication logout(ServletRequest request) {
      return Authentication.UNAUTHENTICATED;
    }
  }

  private static final class RemoteUserIdentity implements UserIdentity {
    private final Subject subject;
    private final Principal principal;

    RemoteUserIdentity(Subject subject, Principal principal) {
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

  private static final class RemoteUserPrincipal implements Principal {
    private final String name;

    RemoteUserPrincipal(String name) {
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
}

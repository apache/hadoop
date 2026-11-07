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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.Request;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestJettyAuthenticationHelper {

  private static HttpServletRequest wrap(Request base, String remoteUser) {
    return new HttpServletRequestWrapper(base) {
      @Override
      public String getRemoteUser() {
        return remoteUser;
      }
    };
  }

  @Test
  public void testSetsAuthenticationFromRemoteUser() {
    Request base = new Request(null, null);
    JettyAuthenticationHelper.publishRemoteUser(wrap(base, "alice"));

    Authentication auth = base.getAuthentication();
    assertInstanceOf(Authentication.User.class, auth);
    Authentication.User user = (Authentication.User) auth;
    assertEquals("alice", user.getUserIdentity().getUserPrincipal().getName());
  }

  @Test
  public void testNullRemoteUserIsNoOp() {
    Request base = new Request(null, null);
    JettyAuthenticationHelper.publishRemoteUser(wrap(base, null));
    assertNull(base.getAuthentication());
  }

  @Test
  public void testEmptyRemoteUserIsNoOp() {
    Request base = new Request(null, null);
    JettyAuthenticationHelper.publishRemoteUser(wrap(base, ""));
    assertNull(base.getAuthentication());
  }

  @Test
  public void testPreservesExistingAuthentication() {
    Request base = new Request(null, null);
    JettyAuthenticationHelper.publishRemoteUser(wrap(base, "eve"));
    Authentication existing = base.getAuthentication();

    JettyAuthenticationHelper.publishRemoteUser(wrap(base, "bob"));

    assertSame(existing, base.getAuthentication());
  }

  @Test
  public void testExplicitUserOverloadSetsAuthentication() {
    Request base = new Request(null, null);
    HttpServletRequest wrapped = wrap(base, null);

    JettyAuthenticationHelper.publishRemoteUser(wrapped, "dave");

    Authentication auth = base.getAuthentication();
    assertInstanceOf(Authentication.User.class, auth);
    assertEquals("dave", ((Authentication.User) auth)
        .getUserIdentity().getUserPrincipal().getName());
  }

  @Test
  public void testNonJettyRequestIsNoOp() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteUser()).thenReturn("carol");
    JettyAuthenticationHelper.publishRemoteUser(request);
  }
}

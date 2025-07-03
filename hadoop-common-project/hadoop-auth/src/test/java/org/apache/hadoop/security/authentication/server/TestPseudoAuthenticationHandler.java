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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.Properties;

public class TestPseudoAuthenticationHandler {

  @Test
  public void testInit() throws Exception {
    PseudoAuthenticationHandler handler = new PseudoAuthenticationHandler();
    try {
      Properties props = new Properties();
      props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "false");
      handler.init(props);
      assertEquals(false, handler.getAcceptAnonymous());
    } finally {
      handler.destroy();
    }
  }

  @Test
  public void testType() throws Exception {
    PseudoAuthenticationHandler handler = new PseudoAuthenticationHandler();
    assertEquals(PseudoAuthenticationHandler.TYPE, handler.getType());
  }

  @Test
  public void testAnonymousOn() throws Exception {
    PseudoAuthenticationHandler handler = new PseudoAuthenticationHandler();
    try {
      Properties props = new Properties();
      props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "true");
      handler.init(props);

      HttpServletRequest request = mock(HttpServletRequest.class);
      HttpServletResponse response = mock(HttpServletResponse.class);

      AuthenticationToken token = handler.authenticate(request, response);

      assertEquals(AuthenticationToken.ANONYMOUS, token);
    } finally {
      handler.destroy();
    }
  }

  @Test
  public void testAnonymousOff() throws Exception {
    PseudoAuthenticationHandler handler = new PseudoAuthenticationHandler();
    try {
      Properties props = new Properties();
      props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "false");
      handler.init(props);

      HttpServletRequest request = mock(HttpServletRequest.class);
      HttpServletResponse response = mock(HttpServletResponse.class);

      AuthenticationToken token = handler.authenticate(request, response);
      assertNull(token);
    } finally {
      handler.destroy();
    }
  }

  private void _testUserName(boolean anonymous) throws Exception {
    PseudoAuthenticationHandler handler = new PseudoAuthenticationHandler();
    try {
      Properties props = new Properties();
      props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, Boolean.toString(anonymous));
      handler.init(props);

      HttpServletRequest request = mock(HttpServletRequest.class);
      HttpServletResponse response = mock(HttpServletResponse.class);
      when(request.getQueryString()).thenReturn(PseudoAuthenticator.USER_NAME + "=" + "user");

      AuthenticationToken token = handler.authenticate(request, response);

      assertNotNull(token);
      assertEquals("user", token.getUserName());
      assertEquals("user", token.getName());
      assertEquals(PseudoAuthenticationHandler.TYPE, token.getType());
    } finally {
      handler.destroy();
    }
  }

  @Test
  public void testUserNameAnonymousOff() throws Exception {
    _testUserName(false);
  }

  @Test
  public void testUserNameAnonymousOn() throws Exception {
    _testUserName(true);
  }

}

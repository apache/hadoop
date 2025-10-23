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
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.apache.hadoop.security.authentication.server.LdapAuthenticationHandler.*;
import static org.apache.hadoop.security.authentication.server.LdapConstants.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifs;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.ApacheDSTestExtension;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * This unit test verifies the functionality of LDAP authentication handler.
 */
@ExtendWith(ApacheDSTestExtension.class)
@CreateLdapServer(
    transports =
      {
        @CreateTransport(protocol = "LDAP", address= LDAP_SERVER_ADDR),
      })
@CreateDS(allowAnonAccess = true,
          partitions = {
            @CreatePartition(
                name = "Test_Partition", suffix = LDAP_BASE_DN,
                contextEntry = @ContextEntry(
                    entryLdif = "dn: " + LDAP_BASE_DN + " \n"
                              + "dc: example\n"
                              + "objectClass: top\n"
                              + "objectClass: domain\n\n"))})
@ApplyLdifs({
    "dn: uid=bjones," + LDAP_BASE_DN,
    "cn: Bob Jones",
    "sn: Jones",
    "objectClass: inetOrgPerson",
    "uid: bjones",
    "userPassword: p@ssw0rd"})
public class TestLdapAuthenticationHandler extends AbstractLdapTestUnit {
  private LdapAuthenticationHandler handler;

  @BeforeEach
  public void setup() throws Exception {
    handler = new LdapAuthenticationHandler();
    try {
      handler.init(getDefaultProperties());
    } catch (Exception e) {
      handler = null;
      throw e;
    }
  }

  protected Properties getDefaultProperties() {
    Properties p = new Properties();
    p.setProperty(BASE_DN, LDAP_BASE_DN);
    p.setProperty(PROVIDER_URL, String.format("ldap://%s:%s", LDAP_SERVER_ADDR,
        getLdapServer().getPort()));
    return p;
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void testRequestWithoutAuthorization() throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);

    assertNull(handler.authenticate(request, response));
    verify(response).setHeader(WWW_AUTHENTICATE, HttpConstants.BASIC);
    verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void testRequestWithInvalidAuthorization() throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);

    final Base64 base64 = new Base64(0);
    String credentials = "bjones:invalidpassword";
    when(request.getHeader(HttpConstants.AUTHORIZATION_HEADER))
        .thenReturn(base64.encodeToString(credentials.getBytes()));
    assertNull(handler.authenticate(request, response));
    verify(response).setHeader(WWW_AUTHENTICATE, HttpConstants.BASIC);
    verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void testRequestWithIncompleteAuthorization() throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);

    when(request.getHeader(HttpConstants.AUTHORIZATION_HEADER))
        .thenReturn(HttpConstants.BASIC);
    assertNull(handler.authenticate(request, response));
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void testRequestWithAuthorization() throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);

    final Base64 base64 = new Base64(0);
    String credentials = base64.encodeToString("bjones:p@ssw0rd".getBytes());
    String authHeader = HttpConstants.BASIC + " " + credentials;
    when(request.getHeader(HttpConstants.AUTHORIZATION_HEADER))
        .thenReturn(authHeader);
    AuthenticationToken token = handler.authenticate(request, response);
    assertNotNull(token);
    verify(response).setStatus(HttpServletResponse.SC_OK);
    assertEquals(token.getType(), TYPE);
    assertEquals(token.getUserName(), "bjones");
    assertEquals(token.getName(), "bjones");
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void testRequestWithWrongCredentials() throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);

    final Base64 base64 = new Base64(0);
    String credentials = base64.encodeToString("bjones:foo123".getBytes());
    String authHeader = HttpConstants.BASIC + " " + credentials;
    when(request.getHeader(HttpConstants.AUTHORIZATION_HEADER))
        .thenReturn(authHeader);

    try {
      handler.authenticate(request, response);
      fail();
    } catch (AuthenticationException ex) {
      // Expected
    } catch (Exception ex) {
      fail();
    }
  }
}
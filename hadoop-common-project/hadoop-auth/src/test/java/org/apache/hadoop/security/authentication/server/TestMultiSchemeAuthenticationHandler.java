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

import static org.apache.hadoop.security.authentication.server.LdapAuthenticationHandler.BASE_DN;
import static org.apache.hadoop.security.authentication.server.LdapAuthenticationHandler.PROVIDER_URL;
import static org.apache.hadoop.security.authentication.server.LdapAuthenticationHandler.TYPE;
import static org.apache.hadoop.security.authentication.server.MultiSchemeAuthenticationHandler.SCHEMES_PROPERTY;
import static org.apache.hadoop.security.authentication.server.MultiSchemeAuthenticationHandler.AUTH_HANDLER_PROPERTY;
import static org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler.PRINCIPAL;
import static org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler.KEYTAB;
import static org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler.NAME_RULES;
import static org.apache.hadoop.security.authentication.server.LdapConstants.*;
import static org.apache.hadoop.security.authentication.server.HttpConstants.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifs;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.ApacheDSTestExtension;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * This unit test verifies the functionality of "multi-scheme" auth handler.
 */
@ExtendWith(ApacheDSTestExtension.class)
@CreateLdapServer(
    transports =
      {
        @CreateTransport(protocol = "LDAP", address = LDAP_SERVER_ADDR),
      })
@CreateDS(allowAnonAccess = true,
          partitions = {
            @CreatePartition(
              name = "Test_Partition", suffix = LDAP_BASE_DN,
              contextEntry = @ContextEntry(
                  entryLdif = "dn: "+ LDAP_BASE_DN+ " \n"
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
public class TestMultiSchemeAuthenticationHandler
    extends AbstractLdapTestUnit {
  private KerberosSecurityTestcase krbTest = new KerberosSecurityTestcase();
  private MultiSchemeAuthenticationHandler handler;

  @BeforeEach
  public void setUp()  throws Exception {
    krbTest.startMiniKdc();

    // create keytab
    File keytabFile = new File(KerberosTestUtils.getKeytabFile());
    String clientPrinc = KerberosTestUtils.getClientPrincipal();
    String serverPrinc = KerberosTestUtils.getServerPrincipal();
    clientPrinc = clientPrinc.substring(0, clientPrinc.lastIndexOf("@"));
    serverPrinc = serverPrinc.substring(0, serverPrinc.lastIndexOf("@"));
    krbTest.getKdc().createPrincipal(keytabFile, clientPrinc, serverPrinc);
    // configure handler
    handler = new MultiSchemeAuthenticationHandler();
    try {
      handler.init(getDefaultProperties());
    } catch (Exception e) {
      throw e;
    }
  }

  @AfterEach
  public void tearDown() throws Exception {
    krbTest.stopMiniKdc();
  }

  private Properties getDefaultProperties() {
    Properties p = new Properties();
    p.setProperty(SCHEMES_PROPERTY, BASIC + "," + NEGOTIATE);
    p.setProperty(String.format(AUTH_HANDLER_PROPERTY, "negotiate"),
        "kerberos");
    p.setProperty(String.format(AUTH_HANDLER_PROPERTY, "basic"), "ldap");
    // Kerberos related config
    p.setProperty(PRINCIPAL, KerberosTestUtils.getServerPrincipal());
    p.setProperty(KEYTAB, KerberosTestUtils.getKeytabFile());
    p.setProperty(NAME_RULES,
        "RULE:[1:$1@$0](.*@" + KerberosTestUtils.getRealm()+")s/@.*//\n");
    // LDAP related config
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
    verify(response).addHeader(WWW_AUTHENTICATE_HEADER, BASIC);
    verify(response).addHeader(WWW_AUTHENTICATE_HEADER, NEGOTIATE);
    verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void testRequestWithInvalidAuthorization() throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);

    final Base64 base64 = new Base64(0);
    String credentials = "bjones:invalidpassword";
    when(request.getHeader(AUTHORIZATION_HEADER))
        .thenReturn(base64.encodeToString(credentials.getBytes()));
    assertNull(handler.authenticate(request, response));
    verify(response).addHeader(WWW_AUTHENTICATE_HEADER, BASIC);
    verify(response).addHeader(WWW_AUTHENTICATE_HEADER, NEGOTIATE);
    verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void testRequestWithLdapAuthorization() throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);

    final Base64 base64 = new Base64(0);
    String credentials = base64.encodeToString("bjones:p@ssw0rd".getBytes());
    String authHeader = BASIC + " " + credentials;
    when(request.getHeader(AUTHORIZATION_HEADER))
        .thenReturn(authHeader);
    AuthenticationToken token = handler.authenticate(request, response);
    assertNotNull(token);
    verify(response).setStatus(HttpServletResponse.SC_OK);
    assertEquals(TYPE, token.getType());
    assertEquals(token.getUserName(), "bjones");
    assertEquals(token.getName(), "bjones");
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void testRequestWithInvalidKerberosAuthorization() throws Exception {
    String token = new Base64(0).encodeToString(new byte[]{0, 1, 2});

    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);

    when(request.getHeader(AUTHORIZATION_HEADER)).thenReturn(
        NEGOTIATE + token);

    try {
      handler.authenticate(request, response);
      fail();
    } catch (AuthenticationException ex) {
      // Expected
    } catch (Exception ex) {
      fail("Wrong exception :"+ex);
    }
  }

}

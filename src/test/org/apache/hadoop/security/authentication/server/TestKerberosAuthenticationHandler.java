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

import static org.junit.Assert.*;

import java.util.Properties;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

//Disabled because kerberos setup and valid keytabs are required.
@Ignore("requires kerberos setup")
public class TestKerberosAuthenticationHandler {

  private KerberosAuthenticationHandler handler;

  @Before
  public void setUp() throws Exception {
    handler = new KerberosAuthenticationHandler();
    Properties props = new Properties();
    props.setProperty(KerberosAuthenticationHandler.PRINCIPAL, KerberosTestUtils.getServerPrincipal());
    props.setProperty(KerberosAuthenticationHandler.KEYTAB, KerberosTestUtils.getKeytabFile());
    props.setProperty(KerberosAuthenticationHandler.NAME_RULES,
                      "RULE:[1:$1@$0](.*@" + KerberosTestUtils.getRealm()+")s/@.*//\n");
    try {
      handler.init(props);
    } catch (Exception ex) {
      handler = null;
      throw ex;
    }
  }

  @After
  public void tearDown() throws Exception {
    if (handler != null) {
      handler.destroy();
      handler = null;
    }
  }

  @Test
  public void testInit() throws Exception {
    assertEquals(KerberosTestUtils.getServerPrincipal(), handler.getPrincipal());
    assertEquals(KerberosTestUtils.getKeytabFile(), handler.getKeytab());
  }

  @Test
  public void testType() throws Exception {
    KerberosAuthenticationHandler handler = new KerberosAuthenticationHandler();
    assertEquals(KerberosAuthenticationHandler.TYPE, handler.getType());
  }

  @Test
  public void testRequestWithoutAuthorization() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    assertNull(handler.authenticate(request, response));
    Mockito.verify(response).setHeader(KerberosAuthenticator.WWW_AUTHENTICATE, KerberosAuthenticator.NEGOTIATE);
    Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  public void testRequestWithInvalidAuthorization() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    Mockito.when(request.getHeader(KerberosAuthenticator.AUTHORIZATION)).thenReturn("invalid");
    assertNull(handler.authenticate(request, response));
    Mockito.verify(response).setHeader(KerberosAuthenticator.WWW_AUTHENTICATE, KerberosAuthenticator.NEGOTIATE);
    Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  public void testRequestWithIncompleteAuthorization() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    Mockito.when(request.getHeader(KerberosAuthenticator.AUTHORIZATION))
      .thenReturn(KerberosAuthenticator.NEGOTIATE);
    try {
      handler.authenticate(request, response);
      fail();
    } catch (AuthenticationException ex) {
      // Expected
    } catch (Exception ex) {
      fail();
    }
  }

  @Test
  public void testRequestWithAuthorization() throws Exception {
    String token = KerberosTestUtils.doAsClient(new Callable<String>() {
      @Override
      public String call() throws Exception {
        GSSManager gssManager = GSSManager.getInstance();
        GSSContext gssContext = null;
        try {
          String servicePrincipal = KerberosTestUtils.getServerPrincipal();
          Oid oid = KerberosUtil.getOidInstance("NT_GSS_KRB5_PRINCIPAL");
          GSSName serviceName = gssManager.createName(servicePrincipal,
              oid);
          oid = KerberosUtil.getOidInstance("GSS_KRB5_MECH_OID");
          gssContext = gssManager.createContext(serviceName, oid, null,
                                                  GSSContext.DEFAULT_LIFETIME);
          gssContext.requestCredDeleg(true);
          gssContext.requestMutualAuth(true);

          byte[] inToken = new byte[0];
          byte[] outToken = gssContext.initSecContext(inToken, 0, inToken.length);
          Base64 base64 = new Base64(0);
          return base64.encodeToString(outToken);

        } finally {
          if (gssContext != null) {
            gssContext.dispose();
          }
        }
      }
    });

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    Mockito.when(request.getHeader(KerberosAuthenticator.AUTHORIZATION))
      .thenReturn(KerberosAuthenticator.NEGOTIATE + " " + token);

    AuthenticationToken authToken = handler.authenticate(request, response);

    if (authToken != null) {
      Mockito.verify(response).setHeader(Mockito.eq(KerberosAuthenticator.WWW_AUTHENTICATE),
                                         Mockito.matches(KerberosAuthenticator.NEGOTIATE + " .*"));
      Mockito.verify(response).setStatus(HttpServletResponse.SC_OK);

      assertEquals(KerberosTestUtils.getClientPrincipal(), authToken.getName());
      assertTrue(KerberosTestUtils.getClientPrincipal().startsWith(authToken.getUserName()));
      assertEquals(KerberosAuthenticationHandler.TYPE, authToken.getType());
    } else {
      Mockito.verify(response).setHeader(Mockito.eq(KerberosAuthenticator.WWW_AUTHENTICATE),
                                         Mockito.matches(KerberosAuthenticator.NEGOTIATE + " .*"));
      Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }
  }

  @Test
  public void testRequestWithInvalidKerberosAuthorization() throws Exception {

    String token = new Base64(0).encodeToString(new byte[]{0, 1, 2});

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    Mockito.when(request.getHeader(KerberosAuthenticator.AUTHORIZATION)).thenReturn(
      KerberosAuthenticator.NEGOTIATE + token);

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

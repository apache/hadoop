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
package org.apache.hadoop.security.authentication.client;

import static org.apache.hadoop.security.authentication.server.MultiSchemeAuthenticationHandler.SCHEMES_PROPERTY;
import static org.apache.hadoop.security.authentication.server.MultiSchemeAuthenticationHandler.AUTH_HANDLER_PROPERTY;
import static org.apache.hadoop.security.authentication.server.AuthenticationFilter.AUTH_TYPE;
import static org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler.PRINCIPAL;
import static org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler.KEYTAB;
import static org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler.NAME_RULES;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import javax.security.sasl.AuthenticationException;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.MultiSchemeAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Test class for {@link KerberosAuthenticator}.
 */
public class TestKerberosAuthenticator extends KerberosSecurityTestcase {

  public TestKerberosAuthenticator() {
  }

  @Before
  public void setup() throws Exception {
    // create keytab
    File keytabFile = new File(KerberosTestUtils.getKeytabFile());
    String clientPrincipal = KerberosTestUtils.getClientPrincipal();
    String serverPrincipal = KerberosTestUtils.getServerPrincipal();
    clientPrincipal = clientPrincipal.substring(0, clientPrincipal.lastIndexOf("@"));
    serverPrincipal = serverPrincipal.substring(0, serverPrincipal.lastIndexOf("@"));
    getKdc().createPrincipal(keytabFile, clientPrincipal, serverPrincipal);
  }

  private Properties getAuthenticationHandlerConfiguration() {
    Properties props = new Properties();
    props.setProperty(AuthenticationFilter.AUTH_TYPE, "kerberos");
    props.setProperty(KerberosAuthenticationHandler.PRINCIPAL, KerberosTestUtils.getServerPrincipal());
    props.setProperty(KerberosAuthenticationHandler.KEYTAB, KerberosTestUtils.getKeytabFile());
    props.setProperty(KerberosAuthenticationHandler.NAME_RULES,
                      "RULE:[1:$1@$0](.*@" + KerberosTestUtils.getRealm()+")s/@.*//\n");
    props.setProperty(KerberosAuthenticationHandler.RULE_MECHANISM, "hadoop");
    return props;
  }

  private Properties getMultiAuthHandlerConfiguration() {
    Properties props = new Properties();
    props.setProperty(AUTH_TYPE, MultiSchemeAuthenticationHandler.TYPE);
    props.setProperty(SCHEMES_PROPERTY, "negotiate");
    props.setProperty(String.format(AUTH_HANDLER_PROPERTY, "negotiate"),
        "kerberos");
    props.setProperty(PRINCIPAL, KerberosTestUtils.getServerPrincipal());
    props.setProperty(KEYTAB, KerberosTestUtils.getKeytabFile());
    props.setProperty(NAME_RULES,
        "RULE:[1:$1@$0](.*@" + KerberosTestUtils.getRealm() + ")s/@.*//\n");
    return props;
  }

  @Test(timeout=60000)
  public void testFallbacktoPseudoAuthenticator() throws Exception {
    AuthenticatorTestCase auth = new AuthenticatorTestCase();
    Properties props = new Properties();
    props.setProperty(AuthenticationFilter.AUTH_TYPE, "simple");
    props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "false");
    AuthenticatorTestCase.setAuthenticationHandlerConfig(props);
    auth._testAuthentication(new KerberosAuthenticator(), false);
  }

  @Test(timeout=60000)
  public void testFallbacktoPseudoAuthenticatorAnonymous() throws Exception {
    AuthenticatorTestCase auth = new AuthenticatorTestCase();
    Properties props = new Properties();
    props.setProperty(AuthenticationFilter.AUTH_TYPE, "simple");
    props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "true");
    AuthenticatorTestCase.setAuthenticationHandlerConfig(props);
    auth._testAuthentication(new KerberosAuthenticator(), false);
  }

  @Test(timeout=60000)
  public void testNotAuthenticated() throws Exception {
    AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase.setAuthenticationHandlerConfig(getAuthenticationHandlerConfiguration());
    auth.start();
    try {
      URL url = new URL(auth.getBaseURL());
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
      Assert.assertTrue(conn.getHeaderField(KerberosAuthenticator.WWW_AUTHENTICATE) != null);
    } finally {
      auth.stop();
    }
  }

  @Test(timeout=60000)
  public void testAuthentication() throws Exception {
    final AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase.setAuthenticationHandlerConfig(
            getAuthenticationHandlerConfiguration());
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        auth._testAuthentication(new KerberosAuthenticator(), false);
        return null;
      }
    });
  }

  @Test(timeout=60000)
  public void testAuthenticationPost() throws Exception {
    final AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase.setAuthenticationHandlerConfig(
            getAuthenticationHandlerConfiguration());
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        auth._testAuthentication(new KerberosAuthenticator(), true);
        return null;
      }
    });
  }

  @Test(timeout=60000)
  public void testAuthenticationHttpClient() throws Exception {
    final AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase.setAuthenticationHandlerConfig(
            getAuthenticationHandlerConfiguration());
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        auth._testAuthenticationHttpClient(new KerberosAuthenticator(), false);
        return null;
      }
    });
  }

  @Test(timeout=60000)
  public void testAuthenticationHttpClientPost() throws Exception {
    final AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase.setAuthenticationHandlerConfig(
            getAuthenticationHandlerConfiguration());
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        auth._testAuthenticationHttpClient(new KerberosAuthenticator(), true);
        return null;
      }
    });
  }

  @Test(timeout = 60000)
  public void testNotAuthenticatedWithMultiAuthHandler() throws Exception {
    AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase
        .setAuthenticationHandlerConfig(getMultiAuthHandlerConfiguration());
    auth.start();
    try {
      URL url = new URL(auth.getBaseURL());
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.connect();
      Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED,
          conn.getResponseCode());
      Assert.assertTrue(conn
          .getHeaderField(KerberosAuthenticator.WWW_AUTHENTICATE) != null);
    } finally {
      auth.stop();
    }
  }

  @Test(timeout = 60000)
  public void testAuthenticationWithMultiAuthHandler() throws Exception {
    final AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase
        .setAuthenticationHandlerConfig(getMultiAuthHandlerConfiguration());
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        auth._testAuthentication(new KerberosAuthenticator(), false);
        return null;
      }
    });
  }

  @Test(timeout = 60000)
  public void testAuthenticationHttpClientPostWithMultiAuthHandler()
      throws Exception {
    final AuthenticatorTestCase auth = new AuthenticatorTestCase();
    AuthenticatorTestCase
        .setAuthenticationHandlerConfig(getMultiAuthHandlerConfiguration());
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        auth._testAuthenticationHttpClient(new KerberosAuthenticator(), true);
        return null;
      }
    });
  }

  @Test(timeout = 60000)
  public void testWrapExceptionWithMessage() {
    IOException ex;
    ex = new IOException("Induced exception");
    ex = KerberosAuthenticator.wrapExceptionWithMessage(ex, "Error while "
        + "authenticating with endpoint: localhost");
    Assert.assertEquals("Induced exception", ex.getCause().getMessage());
    Assert.assertEquals("Error while authenticating with endpoint: localhost",
        ex.getMessage());

    ex = new AuthenticationException("Auth exception");
    ex = KerberosAuthenticator.wrapExceptionWithMessage(ex, "Error while "
        + "authenticating with endpoint: localhost");
    Assert.assertEquals("Auth exception", ex.getCause().getMessage());
    Assert.assertEquals("Error while authenticating with endpoint: localhost",
        ex.getMessage());

    // Test for Exception with  no (String) constructor
    // redirect the LOG to and check log message
    ex = new CharacterCodingException();
    Exception ex2 = KerberosAuthenticator.wrapExceptionWithMessage(ex,
        "Error while authenticating with endpoint: localhost");
    Assert.assertTrue(ex instanceof CharacterCodingException);
    Assert.assertTrue(ex.equals(ex2));
  }

}

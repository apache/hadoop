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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.HttpCookie;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.util.Signer;
import org.apache.hadoop.security.authentication.util.SignerSecretProvider;
import org.apache.hadoop.security.authentication.util.StringSignerSecretProviderCreator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class TestAuthenticationFilter {

  private static final long TOKEN_VALIDITY_SEC = 1000;
  private static final long TOKEN_MAX_INACTIVE_INTERVAL = 1000;

  @Test
  public void testGetConfiguration() throws Exception {
    AuthenticationFilter filter = new AuthenticationFilter();
    FilterConfig config = Mockito.mock(FilterConfig.class);
    Mockito.when(config.getInitParameter(AuthenticationFilter.CONFIG_PREFIX)).thenReturn("");
    Mockito.when(config.getInitParameter("a")).thenReturn("A");
    Mockito.when(config.getInitParameterNames()).thenReturn(new Vector<String>(Arrays.asList("a")).elements());
    Properties props = filter.getConfiguration("", config);
    Assert.assertEquals("A", props.getProperty("a"));

    config = Mockito.mock(FilterConfig.class);
    Mockito.when(config.getInitParameter(AuthenticationFilter.CONFIG_PREFIX)).thenReturn("foo");
    Mockito.when(config.getInitParameter("foo.a")).thenReturn("A");
    Mockito.when(config.getInitParameterNames()).thenReturn(new Vector<String>(Arrays.asList("foo.a")).elements());
    props = filter.getConfiguration("foo.", config);
    Assert.assertEquals("A", props.getProperty("a"));
  }

  @Test
  public void testInitEmpty() throws Exception {
    AuthenticationFilter filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameterNames()).thenReturn(new Vector<String>().elements());
      filter.init(config);
      Assert.fail();
    } catch (ServletException ex) {
      // Expected
      Assert.assertEquals("Authentication type must be specified: simple|kerberos|<class>", 
          ex.getMessage());
    } catch (Exception ex) {
      Assert.fail();
    } finally {
      filter.destroy();
    }
  }

  public static class DummyAuthenticationHandler implements AuthenticationHandler {
    public static boolean init;
    public static boolean managementOperationReturn;
    public static boolean destroy;
    public static boolean expired;

    public static final String TYPE = "dummy";

    public static void reset() {
      init = false;
      destroy = false;
    }

    @Override
    public void init(Properties config) throws ServletException {
      init = true;
      managementOperationReturn =
        config.getProperty("management.operation.return", "true").equals("true");
      expired = config.getProperty("expired.token", "false").equals("true");
    }

    @Override
    public boolean managementOperation(AuthenticationToken token,
                                       HttpServletRequest request,
                                       HttpServletResponse response)
      throws IOException, AuthenticationException {
      if (!managementOperationReturn) {
        response.setStatus(HttpServletResponse.SC_ACCEPTED);
      }
      return managementOperationReturn;
    }

    @Override
    public void destroy() {
      destroy = true;
    }

    @Override
    public String getType() {
      return TYPE;
    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException {
      AuthenticationToken token = null;
      String param = request.getParameter("authenticated");
      if (param != null && param.equals("true")) {
        token = new AuthenticationToken("u", "p", "t");
        token.setExpires((expired) ? 0 : System.currentTimeMillis() + TOKEN_VALIDITY_SEC);
      } else {
        if (request.getHeader("WWW-Authenticate") == null) {
          response.setHeader("WWW-Authenticate", "dummyauth");
        } else {
          throw new AuthenticationException("AUTH FAILED");
        }
      }
      return token;
    }
  }

  @Test
  public void testFallbackToRandomSecretProvider() throws Exception {
    // minimal configuration & simple auth handler (Pseudo)
    AuthenticationFilter filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn("simple");
      Mockito.when(config.getInitParameter(
          AuthenticationFilter.AUTH_TOKEN_VALIDITY)).thenReturn(
          (new Long(TOKEN_VALIDITY_SEC)).toString());
      Mockito.when(config.getInitParameterNames()).thenReturn(
          new Vector<>(Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                                     AuthenticationFilter.AUTH_TOKEN_VALIDITY)).elements());
      ServletContext context = Mockito.mock(ServletContext.class);
      Mockito.when(context.getAttribute(AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE))
          .thenReturn(null);
      Mockito.when(config.getServletContext()).thenReturn(context);
      filter.init(config);
      Assert.assertEquals(PseudoAuthenticationHandler.class, filter.getAuthenticationHandler().getClass());
      Assert.assertTrue(filter.isRandomSecret());
      Assert.assertFalse(filter.isCustomSignerSecretProvider());
      Assert.assertNull(filter.getCookieDomain());
      Assert.assertNull(filter.getCookiePath());
      Assert.assertEquals(TOKEN_VALIDITY_SEC, filter.getValidity());
    } finally {
      filter.destroy();
    }
  }
  @Test
  public void testInit() throws Exception {
    // custom secret as inline
    AuthenticationFilter filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn("simple");
      Mockito.when(config.getInitParameterNames()).thenReturn(
          new Vector<>(Arrays.asList(AuthenticationFilter.AUTH_TYPE))
              .elements());
      ServletContext context = Mockito.mock(ServletContext.class);
      Mockito.when(context.getAttribute(
          AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE)).thenReturn(
            new SignerSecretProvider() {
              @Override
              public void init(Properties config, ServletContext servletContext,
                      long tokenValidity) {
              }
              @Override
              public byte[] getCurrentSecret() {
                return null;
              }
              @Override
              public byte[][] getAllSecrets() {
                return null;
              }
            });
      Mockito.when(config.getServletContext()).thenReturn(context);
      filter.init(config);
      Assert.assertFalse(filter.isRandomSecret());
      Assert.assertTrue(filter.isCustomSignerSecretProvider());
    } finally {
      filter.destroy();
    }

    // custom secret by file
    File testDir = new File(System.getProperty("test.build.data",
        "target/test-dir"));
    testDir.mkdirs();
    String secretValue = "hadoop";
    File secretFile = new File(testDir, "http-secret.txt");
    Writer writer = new FileWriter(secretFile);
    writer.write(secretValue);
    writer.close();

    filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter(
          AuthenticationFilter.AUTH_TYPE)).thenReturn("simple");
      Mockito.when(config.getInitParameter(
          AuthenticationFilter.SIGNATURE_SECRET_FILE))
          .thenReturn(secretFile.getAbsolutePath());
      Mockito.when(config.getInitParameterNames()).thenReturn(
          new Vector<String>(Arrays.asList(AuthenticationFilter.AUTH_TYPE,
              AuthenticationFilter.SIGNATURE_SECRET_FILE)).elements());
      ServletContext context = Mockito.mock(ServletContext.class);
      Mockito.when(context.getAttribute(
          AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE))
          .thenReturn(null);
      Mockito.when(config.getServletContext()).thenReturn(context);
      filter.init(config);
      Assert.assertFalse(filter.isRandomSecret());
      Assert.assertFalse(filter.isCustomSignerSecretProvider());
    } finally {
      filter.destroy();
    }

    // custom cookie domain and cookie path
    filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn("simple");
      Mockito.when(config.getInitParameter(AuthenticationFilter.COOKIE_DOMAIN)).thenReturn(".foo.com");
      Mockito.when(config.getInitParameter(AuthenticationFilter.COOKIE_PATH)).thenReturn("/bar");
      Mockito.when(config.getInitParameterNames()).thenReturn(
        new Vector<String>(Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                                 AuthenticationFilter.COOKIE_DOMAIN,
                                 AuthenticationFilter.COOKIE_PATH)).elements());
      getMockedServletContextWithStringSigner(config);
      filter.init(config);
      Assert.assertEquals(".foo.com", filter.getCookieDomain());
      Assert.assertEquals("/bar", filter.getCookiePath());
    } finally {
      filter.destroy();
    }

    // authentication handler lifecycle, and custom impl
    DummyAuthenticationHandler.reset();
    filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter("management.operation.return")).
        thenReturn("true");
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
        DummyAuthenticationHandler.class.getName());
      Mockito.when(config.getInitParameterNames()).thenReturn(
        new Vector<String>(
          Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                        "management.operation.return")).elements());
      getMockedServletContextWithStringSigner(config);
      filter.init(config);
      Assert.assertTrue(DummyAuthenticationHandler.init);
    } finally {
      filter.destroy();
      Assert.assertTrue(DummyAuthenticationHandler.destroy);
    }

    // kerberos auth handler
    filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      ServletContext sc = Mockito.mock(ServletContext.class);
      Mockito.when(config.getServletContext()).thenReturn(sc);
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn("kerberos");
      Mockito.when(config.getInitParameterNames()).thenReturn(
        new Vector<String>(Arrays.asList(AuthenticationFilter.AUTH_TYPE)).elements());
      filter.init(config);
    } catch (ServletException ex) {
      // Expected
    } finally {
      Assert.assertEquals(KerberosAuthenticationHandler.class, filter.getAuthenticationHandler().getClass());
      filter.destroy();
    }
  }
  
  @Test
  public void testInitCaseSensitivity() throws Exception {
    // minimal configuration & simple auth handler (Pseudo)
    AuthenticationFilter filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn("SimPle");
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TOKEN_VALIDITY)).thenReturn(
          (new Long(TOKEN_VALIDITY_SEC)).toString());
      Mockito.when(config.getInitParameterNames()).thenReturn(
          new Vector<String>(Arrays.asList(AuthenticationFilter.AUTH_TYPE,
              AuthenticationFilter.AUTH_TOKEN_VALIDITY)).elements());
      getMockedServletContextWithStringSigner(config);

      filter.init(config);
      Assert.assertEquals(PseudoAuthenticationHandler.class, 
          filter.getAuthenticationHandler().getClass());
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void testGetRequestURL() throws Exception {
    AuthenticationFilter filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter("management.operation.return")).
        thenReturn("true");
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
        DummyAuthenticationHandler.class.getName());
      Mockito.when(config.getInitParameterNames()).thenReturn(
        new Vector<String>(
          Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                        "management.operation.return")).elements());
      getMockedServletContextWithStringSigner(config);
      filter.init(config);

      HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
      Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer("http://foo:8080/bar"));
      Mockito.when(request.getQueryString()).thenReturn("a=A&b=B");

      Assert.assertEquals("http://foo:8080/bar?a=A&b=B", filter.getRequestURL(request));
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void testGetToken() throws Exception {
    AuthenticationFilter filter = new AuthenticationFilter();

    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter("management.operation.return")).
        thenReturn("true");
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
        DummyAuthenticationHandler.class.getName());
      Mockito.when(config.getInitParameter(AuthenticationFilter.SIGNATURE_SECRET)).thenReturn("secret");
      Mockito.when(config.getInitParameterNames()).thenReturn(
        new Vector<String>(
          Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                        AuthenticationFilter.SIGNATURE_SECRET,
                        "management.operation.return")).elements());
      SignerSecretProvider secretProvider =
          getMockedServletContextWithStringSigner(config);
      filter.init(config);

      AuthenticationToken token = new AuthenticationToken("u", "p", DummyAuthenticationHandler.TYPE);
      token.setExpires(System.currentTimeMillis() + TOKEN_VALIDITY_SEC);

      Signer signer = new Signer(secretProvider);
      String tokenSigned = signer.sign(token.toString());

      Cookie cookie = new Cookie(AuthenticatedURL.AUTH_COOKIE, tokenSigned);
      HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
      Mockito.when(request.getCookies()).thenReturn(new Cookie[]{cookie});

      AuthenticationToken newToken = filter.getToken(request);

      Assert.assertEquals(token.toString(), newToken.toString());
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void testGetTokenExpired() throws Exception {
    AuthenticationFilter filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter("management.operation.return")).thenReturn("true");
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
        DummyAuthenticationHandler.class.getName());
      Mockito.when(config.getInitParameter(AuthenticationFilter.SIGNATURE_SECRET)).thenReturn("secret");
      Mockito.when(config.getInitParameterNames()).thenReturn(
        new Vector<String>(
          Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                        AuthenticationFilter.SIGNATURE_SECRET,
                        "management.operation.return")).elements());
      getMockedServletContextWithStringSigner(config);
      filter.init(config);

      AuthenticationToken token =
          new AuthenticationToken("u", "p", DummyAuthenticationHandler.TYPE);
      token.setExpires(System.currentTimeMillis() - TOKEN_VALIDITY_SEC);
      SignerSecretProvider secretProvider =
          StringSignerSecretProviderCreator.newStringSignerSecretProvider();
      Properties secretProviderProps = new Properties();
      secretProviderProps.setProperty(
              AuthenticationFilter.SIGNATURE_SECRET, "secret");
      secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
      Signer signer = new Signer(secretProvider);
      String tokenSigned = signer.sign(token.toString());

      Cookie cookie = new Cookie(AuthenticatedURL.AUTH_COOKIE, tokenSigned);
      HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
      Mockito.when(request.getCookies()).thenReturn(new Cookie[]{cookie});

      boolean failed = false;
      try {
        filter.getToken(request);
      } catch (AuthenticationException ex) {
        Assert.assertEquals("AuthenticationToken expired", ex.getMessage());
        failed = true;
      } finally {
        Assert.assertTrue("token not expired", failed);
      }
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void testGetTokenInvalidType() throws Exception {
    AuthenticationFilter filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter("management.operation.return")).
        thenReturn("true");
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
        DummyAuthenticationHandler.class.getName());
      Mockito.when(config.getInitParameter(AuthenticationFilter.SIGNATURE_SECRET)).thenReturn("secret");
      Mockito.when(config.getInitParameterNames()).thenReturn(
        new Vector<String>(
          Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                        AuthenticationFilter.SIGNATURE_SECRET,
                        "management.operation.return")).elements());
      getMockedServletContextWithStringSigner(config);
      filter.init(config);

      AuthenticationToken token = new AuthenticationToken("u", "p", "invalidtype");
      token.setExpires(System.currentTimeMillis() + TOKEN_VALIDITY_SEC);
      SignerSecretProvider secretProvider =
          StringSignerSecretProviderCreator.newStringSignerSecretProvider();
      Properties secretProviderProps = new Properties();
      secretProviderProps.setProperty(
              AuthenticationFilter.SIGNATURE_SECRET, "secret");
      secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
      Signer signer = new Signer(secretProvider);
      String tokenSigned = signer.sign(token.toString());

      Cookie cookie = new Cookie(AuthenticatedURL.AUTH_COOKIE, tokenSigned);
      HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
      Mockito.when(request.getCookies()).thenReturn(new Cookie[]{cookie});

      boolean failed = false;
      try {
        filter.getToken(request);
      } catch (AuthenticationException ex) {
        Assert.assertEquals("Invalid AuthenticationToken type", ex.getMessage());
        failed = true;
      } finally {
        Assert.assertTrue("token not invalid type", failed);
      }
    } finally {
      filter.destroy();
    }
  }

  private static SignerSecretProvider getMockedServletContextWithStringSigner(
      FilterConfig config) throws Exception {
    Properties secretProviderProps = new Properties();
    secretProviderProps.setProperty(AuthenticationFilter.SIGNATURE_SECRET,
                                    "secret");
    SignerSecretProvider secretProvider =
        StringSignerSecretProviderCreator.newStringSignerSecretProvider();
    secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);

    ServletContext context = Mockito.mock(ServletContext.class);
    Mockito.when(context.getAttribute(
            AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE))
            .thenReturn(secretProvider);
    Mockito.when(config.getServletContext()).thenReturn(context);
    return secretProvider;
  }

  @Test
  public void testDoFilterNotAuthenticated() throws Exception {
    AuthenticationFilter filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter("management.operation.return")).
        thenReturn("true");
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
        DummyAuthenticationHandler.class.getName());
      Mockito.when(config.getInitParameterNames()).thenReturn(
        new Vector<String>(
          Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                        "management.operation.return")).elements());
      getMockedServletContextWithStringSigner(config);
      filter.init(config);

      HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
      Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer("http://foo:8080/bar"));

      HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

      FilterChain chain = Mockito.mock(FilterChain.class);

      Mockito.doAnswer(
        new Answer<Object>() {
          @Override
          public Object answer(InvocationOnMock invocation) throws Throwable {
            Assert.fail();
            return null;
          }
        }
      ).when(chain).doFilter(Mockito.<ServletRequest>anyObject(), Mockito.<ServletResponse>anyObject());

      Mockito.when(response.containsHeader("WWW-Authenticate")).thenReturn(true);
      filter.doFilter(request, response, chain);

      Mockito.verify(response).sendError(
          HttpServletResponse.SC_UNAUTHORIZED, "Authentication required");
    } finally {
      filter.destroy();
    }
  }

  private void _testDoFilterAuthentication(boolean withDomainPath,
                                           boolean invalidToken,
                                           boolean expired) throws Exception {
    AuthenticationFilter filter = new AuthenticationFilter();
    FilterConfig config = Mockito.mock(FilterConfig.class);
    Mockito.when(config.getInitParameter("management.operation.return")).
            thenReturn("true");
    Mockito.when(config.getInitParameter("expired.token")).
            thenReturn(Boolean.toString(expired));
    Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE))
            .thenReturn(DummyAuthenticationHandler.class.getName());
    Mockito.when(config.getInitParameter(AuthenticationFilter
            .AUTH_TOKEN_VALIDITY)).thenReturn(new Long(TOKEN_VALIDITY_SEC).toString());
    Mockito.when(config.getInitParameter(AuthenticationFilter
            .SIGNATURE_SECRET)).thenReturn("secret");
    Mockito.when(config.getInitParameterNames()).thenReturn(new
            Vector<String>(Arrays.asList(AuthenticationFilter.AUTH_TYPE,
            AuthenticationFilter.AUTH_TOKEN_VALIDITY,
            AuthenticationFilter.SIGNATURE_SECRET, "management.operation" +
            ".return", "expired.token")).elements());
    getMockedServletContextWithStringSigner(config);

    if (withDomainPath) {
      Mockito.when(config.getInitParameter(AuthenticationFilter
              .COOKIE_DOMAIN)).thenReturn(".foo.com");
      Mockito.when(config.getInitParameter(AuthenticationFilter.COOKIE_PATH))
              .thenReturn("/bar");
      Mockito.when(config.getInitParameterNames()).thenReturn(new
              Vector<String>(Arrays.asList(AuthenticationFilter.AUTH_TYPE,
              AuthenticationFilter.AUTH_TOKEN_VALIDITY,
              AuthenticationFilter.SIGNATURE_SECRET,
              AuthenticationFilter.COOKIE_DOMAIN, AuthenticationFilter
              .COOKIE_PATH, "management.operation.return")).elements());
    }

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getParameter("authenticated")).thenReturn("true");
    Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer
            ("http://foo:8080/bar"));
    Mockito.when(request.getQueryString()).thenReturn("authenticated=true");

    if (invalidToken) {
      Mockito.when(request.getCookies()).thenReturn(new Cookie[]{new Cookie
              (AuthenticatedURL.AUTH_COOKIE, "foo")});
    }

    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    FilterChain chain = Mockito.mock(FilterChain.class);

    final Map<String, String> cookieMap = new HashMap<String, String>();
    Mockito.doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        String cookieHeader = (String)invocation.getArguments()[1];
        parseCookieMap(cookieHeader, cookieMap);
        return null;
      }
    }).when(response).addHeader(Mockito.eq("Set-Cookie"), Mockito.anyString());

    try {
      filter.init(config);
      filter.doFilter(request, response, chain);

      if (expired) {
        Mockito.verify(response, Mockito.never()).
          addHeader(Mockito.eq("Set-Cookie"), Mockito.anyString());
      } else {
        String v = cookieMap.get(AuthenticatedURL.AUTH_COOKIE);
        Assert.assertNotNull("cookie missing", v);
        Assert.assertTrue(v.contains("u=") && v.contains("p=") && v.contains
                ("t=") && v.contains("e=") && v.contains("s="));
        Mockito.verify(chain).doFilter(Mockito.any(ServletRequest.class),
                Mockito.any(ServletResponse.class));

        SignerSecretProvider secretProvider =
            StringSignerSecretProviderCreator.newStringSignerSecretProvider();
        Properties secretProviderProps = new Properties();
        secretProviderProps.setProperty(
                AuthenticationFilter.SIGNATURE_SECRET, "secret");
        secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
        Signer signer = new Signer(secretProvider);
        String value = signer.verifyAndExtract(v);
        AuthenticationToken token = AuthenticationToken.parse(value);
        assertThat(token.getExpires(), not(0L));

        if (withDomainPath) {
          Assert.assertEquals(".foo.com", cookieMap.get("Domain"));
          Assert.assertEquals("/bar", cookieMap.get("Path"));
        } else {
          Assert.assertFalse(cookieMap.containsKey("Domain"));
          Assert.assertFalse(cookieMap.containsKey("Path"));
        }
      }
    } finally {
      filter.destroy();
    }
  }

  private static void parseCookieMap(String cookieHeader, Map<String,
          String> cookieMap) {
    List<HttpCookie> cookies = HttpCookie.parse(cookieHeader);
    for (HttpCookie cookie : cookies) {
      if (AuthenticatedURL.AUTH_COOKIE.equals(cookie.getName())) {
        cookieMap.put(cookie.getName(), cookie.getValue());
        if (cookie.getPath() != null) {
          cookieMap.put("Path", cookie.getPath());
        }
        if (cookie.getDomain() != null) {
          cookieMap.put("Domain", cookie.getDomain());
        }
      }
    }
  }

  @Test
  public void testDoFilterAuthentication() throws Exception {
    _testDoFilterAuthentication(false, false, false);
  }

  @Test
  public void testDoFilterAuthenticationImmediateExpiration() throws Exception {
    _testDoFilterAuthentication(false, false, true);
  }

  @Test
  public void testDoFilterAuthenticationWithInvalidToken() throws Exception {
    _testDoFilterAuthentication(false, true, false);
  }

  @Test
  public void testDoFilterAuthenticationWithDomainPath() throws Exception {
    _testDoFilterAuthentication(true, false, false);
  }

  @Test
  public void testDoFilterAuthenticated() throws Exception {
    AuthenticationFilter filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter("management.operation.return")).
        thenReturn("true");
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
        DummyAuthenticationHandler.class.getName());
      Mockito.when(config.getInitParameterNames()).thenReturn(
        new Vector<String>(
          Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                        "management.operation.return")).elements());
      getMockedServletContextWithStringSigner(config);
      filter.init(config);

      HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
      Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer("http://foo:8080/bar"));

      AuthenticationToken token = new AuthenticationToken("u", "p", "t");
      token.setExpires(System.currentTimeMillis() + TOKEN_VALIDITY_SEC);
      SignerSecretProvider secretProvider =
          StringSignerSecretProviderCreator.newStringSignerSecretProvider();
      Properties secretProviderProps = new Properties();
      secretProviderProps.setProperty(
              AuthenticationFilter.SIGNATURE_SECRET, "secret");
      secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
      Signer signer = new Signer(secretProvider);
      String tokenSigned = signer.sign(token.toString());

      Cookie cookie = new Cookie(AuthenticatedURL.AUTH_COOKIE, tokenSigned);
      Mockito.when(request.getCookies()).thenReturn(new Cookie[]{cookie});

      HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

      FilterChain chain = Mockito.mock(FilterChain.class);

      Mockito.doAnswer(
        new Answer<Object>() {
          @Override
          public Object answer(InvocationOnMock invocation) throws Throwable {
            Object[] args = invocation.getArguments();
            HttpServletRequest request = (HttpServletRequest) args[0];
            Assert.assertEquals("u", request.getRemoteUser());
            Assert.assertEquals("p", request.getUserPrincipal().getName());
            return null;
          }
        }
      ).when(chain).doFilter(Mockito.<ServletRequest>anyObject(), Mockito.<ServletResponse>anyObject());

      filter.doFilter(request, response, chain);

    } finally {
      filter.destroy();
    }
  }

  @Test
  public void testDoFilterAuthenticationFailure() throws Exception {
    AuthenticationFilter filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter("management.operation.return")).
        thenReturn("true");
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
        DummyAuthenticationHandler.class.getName());
      Mockito.when(config.getInitParameterNames()).thenReturn(
        new Vector<String>(
          Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                        "management.operation.return")).elements());
      getMockedServletContextWithStringSigner(config);
      filter.init(config);

      HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
      Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer("http://foo:8080/bar"));
      Mockito.when(request.getCookies()).thenReturn(new Cookie[]{});
      Mockito.when(request.getHeader("WWW-Authenticate")).thenReturn("dummyauth");
      HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

      FilterChain chain = Mockito.mock(FilterChain.class);

      final Map<String, String> cookieMap = new HashMap<String, String>();
      Mockito.doAnswer(
        new Answer<Object>() {
          @Override
          public Object answer(InvocationOnMock invocation) throws Throwable {
            Object[] args = invocation.getArguments();
            parseCookieMap((String) args[1], cookieMap);
            return null;
          }
        }
      ).when(response).addHeader(Mockito.eq("Set-Cookie"), Mockito.anyString());

      Mockito.doAnswer(
        new Answer<Object>() {
          @Override
          public Object answer(InvocationOnMock invocation) throws Throwable {
            Assert.fail("shouldn't get here");
            return null;
          }
        }
      ).when(chain).doFilter(Mockito.<ServletRequest>anyObject(), Mockito.<ServletResponse>anyObject());

      filter.doFilter(request, response, chain);

      Mockito.verify(response).sendError(
          HttpServletResponse.SC_FORBIDDEN, "AUTH FAILED");
      Mockito.verify(response, Mockito.never()).setHeader(Mockito.eq("WWW-Authenticate"), Mockito.anyString());

      String value = cookieMap.get(AuthenticatedURL.AUTH_COOKIE);
      Assert.assertNotNull("cookie missing", value);
      Assert.assertEquals("", value);
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void testDoFilterAuthenticatedExpired() throws Exception {
    String secret = "secret";
    AuthenticationFilter filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter("management.operation.return")).
        thenReturn("true");
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
        DummyAuthenticationHandler.class.getName());
      Mockito.when(config.getInitParameter(AuthenticationFilter.SIGNATURE_SECRET)).thenReturn(
        secret);
      Mockito.when(config.getInitParameterNames()).thenReturn(
        new Vector<String>(
          Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                        AuthenticationFilter.SIGNATURE_SECRET,
                        "management.operation.return")).elements());
      getMockedServletContextWithStringSigner(config);
      filter.init(config);

      HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
      Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer("http://foo:8080/bar"));

      AuthenticationToken token = new AuthenticationToken("u", "p", DummyAuthenticationHandler.TYPE);
      token.setExpires(System.currentTimeMillis() - TOKEN_VALIDITY_SEC);
      SignerSecretProvider secretProvider =
          StringSignerSecretProviderCreator.newStringSignerSecretProvider();
      Properties secretProviderProps = new Properties();
      secretProviderProps.setProperty(
              AuthenticationFilter.SIGNATURE_SECRET, secret);
      secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
      Signer signer = new Signer(secretProvider);
      String tokenSigned = signer.sign(token.toString());

      Cookie cookie = new Cookie(AuthenticatedURL.AUTH_COOKIE, tokenSigned);
      Mockito.when(request.getCookies()).thenReturn(new Cookie[]{cookie});

      HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
      Mockito.when(response.containsHeader("WWW-Authenticate")).thenReturn(true);
      FilterChain chain = Mockito.mock(FilterChain.class);

      verifyUnauthorized(filter, request, response, chain);
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void
  testDoFilterAuthenticationAuthorized() throws Exception {
    // Both expired period and MaxInActiveInterval are not reached.
    long maxInactives = System.currentTimeMillis()
        + TOKEN_MAX_INACTIVE_INTERVAL;
    long expires = System.currentTimeMillis() + TOKEN_VALIDITY_SEC;
    boolean authorized = true;
    _testDoFilterAuthenticationMaxInactiveInterval(maxInactives,
                                                   expires,
                                                   authorized);
  }

  @Test
  public void
  testDoFilterAuthenticationUnauthorizedExpired() throws Exception {
    // Expired period is reached, MaxInActiveInterval is not reached.
    long maxInactives = System.currentTimeMillis()
        + TOKEN_MAX_INACTIVE_INTERVAL;
    long expires = System.currentTimeMillis() - TOKEN_VALIDITY_SEC;
    boolean authorized = false;
    _testDoFilterAuthenticationMaxInactiveInterval(maxInactives,
                                                   expires,
                                                   authorized);
  }

  @Test
  public void
  testDoFilterAuthenticationUnauthorizedInactived() throws Exception {
    // Expired period is not reached, MaxInActiveInterval is reached.
    long maxInactives = System.currentTimeMillis()
        - TOKEN_MAX_INACTIVE_INTERVAL;
    long expires = System.currentTimeMillis() + TOKEN_VALIDITY_SEC;
    boolean authorized = false;
    _testDoFilterAuthenticationMaxInactiveInterval(maxInactives,
                                                   expires,
                                                   authorized);
  }

  @Test
  public void
  testDoFilterAuthenticationUnauthorizedInactivedExpired()
      throws Exception {
    // Both expired period and MaxInActiveInterval is reached.
    long maxInactives = System.currentTimeMillis()
        - TOKEN_MAX_INACTIVE_INTERVAL;
    long expires = System.currentTimeMillis() - TOKEN_VALIDITY_SEC;
    boolean authorized = false;
    _testDoFilterAuthenticationMaxInactiveInterval(maxInactives,
                                                   expires,
                                                   authorized);
  }

  @Test
  public void testTokenWithValidActivityInterval() throws Exception {
    // Provide token containing valid maxInactive value.
    // The token is active.
    // The server has maxInactiveInterval configured to -1.(disabled)
    // The server shall authorize the access, but should not drop a new cookie
    long maxInactives = System.currentTimeMillis()
        + TOKEN_MAX_INACTIVE_INTERVAL;
    long expires = System.currentTimeMillis() + TOKEN_VALIDITY_SEC;
    _testDoFilterAuthenticationMaxInactiveInterval(
        maxInactives,
        -1,
        expires,
        true, //authorized
        false //newCookie
        );
    // Provide token containing valid maxInactive value.
    // The token is active.
    // The server has maxInactiveInterval configured to value
    // greater than 0.(enabled)
    // The server shall authorize the access and drop a new cookie
    // with renewed activity interval
    maxInactives = System.currentTimeMillis()
        + TOKEN_MAX_INACTIVE_INTERVAL;
    expires = System.currentTimeMillis() + TOKEN_VALIDITY_SEC;
    _testDoFilterAuthenticationMaxInactiveInterval(
        maxInactives,
        TOKEN_MAX_INACTIVE_INTERVAL,
        expires,
        true, //authorized
        true //newCookie
        );
  }

  @Test
  public void testTokenWithExpiredActivityIntervaln() throws Exception {
    // Provide token containing invalid maxInactive value.
    // The token is inactive.
    // The server has maxInactiveInterval configured to -1.(disabled)
    // The server should deny access and expire the token.
    long maxInactives = System.currentTimeMillis()
        - TOKEN_MAX_INACTIVE_INTERVAL;
    long expires = System.currentTimeMillis() + TOKEN_VALIDITY_SEC;
    _testDoFilterAuthenticationMaxInactiveInterval(
        maxInactives,
        -1,
        expires,
        false, //authorized
        false //newCookie
        );
    // Provide token containing invalid maxInactive value.
    // The token is inactive.
    // The server has maxInactiveInterval configured to value
    // greater than 0.(enabled)
    // The server should deny access and expire the token.
    maxInactives = System.currentTimeMillis()
        + TOKEN_MAX_INACTIVE_INTERVAL;
    expires = System.currentTimeMillis() + TOKEN_VALIDITY_SEC;
    _testDoFilterAuthenticationMaxInactiveInterval(
        maxInactives,
        -1,
        expires,
        true, //authorized
        false //newCookie
        );
  }

  @Test
  public void testTokenWithNoActivityIntervals()
      throws Exception {
    // Provide token which does not contain maxInactive value.
    // The server has maxInactiveInterval configured to -1.
    // The server shall authorize the access, but should not drop a new cookie
    long expires = System.currentTimeMillis() + TOKEN_VALIDITY_SEC;
    _testDoFilterAuthenticationMaxInactiveInterval(
        -1,
        -1,
        expires,
        true, //authorized
        false //newCookie
        );
    // Provide token which does not contain  maxInactive value.
    // The server has maxInactiveInterval to some value
    // The server shall authorize the access and drop a new cookie
    // with renewed activity interval
    expires = System.currentTimeMillis() + TOKEN_VALIDITY_SEC;
    _testDoFilterAuthenticationMaxInactiveInterval(
        -1,
        TOKEN_MAX_INACTIVE_INTERVAL,
        expires,
        true, //authorized
        true //newCookie
        );
  }

  private void
  _testDoFilterAuthenticationMaxInactiveInterval(long maxInactivesInToken,
                                                 long expires,
                                                 boolean authorized)
                                                     throws Exception {
    _testDoFilterAuthenticationMaxInactiveInterval(maxInactivesInToken,
        TOKEN_MAX_INACTIVE_INTERVAL, expires, authorized, true);
  }

  private void
  _testDoFilterAuthenticationMaxInactiveInterval(long maxInactivesInToken,
                                                 long maxInactivesOnServer,
                                                 long expires,
                                                 boolean authorized,
                                                 boolean newCookie)
                                                     throws Exception {
    String secret = "secret";
    AuthenticationFilter filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter("management.operation.return")).
        thenReturn("true");
      Mockito.when(config.getInitParameter(
          AuthenticationFilter.AUTH_TYPE)).thenReturn(
              DummyAuthenticationHandler.class.getName());
      Mockito.when(config.getInitParameter(
          AuthenticationFilter.SIGNATURE_SECRET)).thenReturn(secret);
      Mockito.when(config.getInitParameter(
          AuthenticationFilter.AUTH_TOKEN_MAX_INACTIVE_INTERVAL)).thenReturn(
              Long.toString(maxInactivesOnServer));
      Mockito.when(config.getInitParameterNames()).thenReturn(
        new Vector<String>(
          Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                        AuthenticationFilter.SIGNATURE_SECRET,
                        AuthenticationFilter.AUTH_TOKEN_MAX_INACTIVE_INTERVAL,
                        "management.operation.return")).elements());
      getMockedServletContextWithStringSigner(config);
      filter.init(config);

      HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
      Mockito.when(request.getRequestURL()).thenReturn(
          new StringBuffer("http://foo:8080/bar"));

      AuthenticationToken token = new AuthenticationToken("u", "p",
          DummyAuthenticationHandler.TYPE);
      token.setMaxInactives(maxInactivesInToken);
      token.setExpires(expires);

      SignerSecretProvider secretProvider =
          StringSignerSecretProviderCreator.newStringSignerSecretProvider();
      Properties secretProviderProps = new Properties();
      secretProviderProps.setProperty(
          AuthenticationFilter.SIGNATURE_SECRET, secret);
      secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
      Signer signer = new Signer(secretProvider);
      String tokenSigned = signer.sign(token.toString());

      Cookie cookie = new Cookie(AuthenticatedURL.AUTH_COOKIE, tokenSigned);
      Mockito.when(request.getCookies()).thenReturn(new Cookie[]{cookie});
      HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
      Mockito.when(response.containsHeader("WWW-Authenticate"))
      .thenReturn(true);
      FilterChain chain = Mockito.mock(FilterChain.class);

      if (authorized) {
        verifyAuthorized(filter, request, response, chain, newCookie);
      } else {
        verifyUnauthorized(filter, request, response, chain);
      }
    } finally {
      filter.destroy();
    }
  }

  private static void verifyAuthorized(AuthenticationFilter filter,
                                       HttpServletRequest request,
                                       HttpServletResponse response,
                                       FilterChain chain,
                                       boolean newCookie) throws
                                                          Exception {
    final Map<String, String> cookieMap = new HashMap<>();
    Mockito.doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        String cookieHeader = (String) invocation.getArguments()[1];
        parseCookieMap(cookieHeader, cookieMap);
        return null;
      }
    }).when(response).addHeader(Mockito.eq("Set-Cookie"), Mockito.anyString());

    filter.doFilter(request, response, chain);

    if (newCookie) {
      // a new cookie should be dropped when maxInactiveInterval is enabled
      String v = cookieMap.get(AuthenticatedURL.AUTH_COOKIE);
      Assert.assertNotNull("cookie missing", v);
      Assert.assertTrue(v.contains("u=") && v.contains("p=") && v.contains
          ("t=") && v.contains("i=") && v.contains("e=")
          && v.contains("s="));
      Mockito.verify(chain).doFilter(Mockito.any(ServletRequest.class),
          Mockito.any(ServletResponse.class));

      SignerSecretProvider secretProvider =
          StringSignerSecretProviderCreator.newStringSignerSecretProvider();
      Properties secretProviderProps = new Properties();
      secretProviderProps.setProperty(
          AuthenticationFilter.SIGNATURE_SECRET, "secret");
      secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
      Signer signer = new Signer(secretProvider);
      String value = signer.verifyAndExtract(v);
      AuthenticationToken token = AuthenticationToken.parse(value);
      assertThat(token.getMaxInactives(), not(0L));
      assertThat(token.getExpires(), not(0L));
      Assert.assertFalse("Token is expired.", token.isExpired());
    } else {
      //make sure that no auth cookie is dropped.
      //For unauthorized response, auth cookie is dropped with empty value
      Assert.assertTrue("cookie is present",
          !cookieMap.containsKey(AuthenticatedURL.AUTH_COOKIE));
    }
  }

  private static void verifyUnauthorized(AuthenticationFilter filter,
                                         HttpServletRequest request,
                                         HttpServletResponse response,
                                         FilterChain chain) throws
                                                            IOException,
                                                            ServletException {
    //For unauthorized response, a cookie is dropped with empty string as value
    final Map<String, String> cookieMap = new HashMap<String, String>();
    Mockito.doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        String cookieHeader = (String) invocation.getArguments()[1];
        parseCookieMap(cookieHeader, cookieMap);
        return null;
      }
    }).when(response).addHeader(Mockito.eq("Set-Cookie"), Mockito.anyString());

    filter.doFilter(request, response, chain);

    Mockito.verify(response).sendError(Mockito.eq(HttpServletResponse
            .SC_UNAUTHORIZED), Mockito.anyString());
    Mockito.verify(chain, Mockito.never()).doFilter(Mockito.any
            (ServletRequest.class), Mockito.any(ServletResponse.class));

    Assert.assertTrue("cookie is missing",
        cookieMap.containsKey(AuthenticatedURL.AUTH_COOKIE));
    Assert.assertEquals("", cookieMap.get(AuthenticatedURL.AUTH_COOKIE));
  }

  @Test
  public void testDoFilterAuthenticatedInvalidType() throws Exception {
    String secret = "secret";
    AuthenticationFilter filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter("management.operation.return")).
        thenReturn("true");
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
        DummyAuthenticationHandler.class.getName());
      Mockito.when(config.getInitParameter(AuthenticationFilter.SIGNATURE_SECRET)).thenReturn(
        secret);
      Mockito.when(config.getInitParameterNames()).thenReturn(
        new Vector<String>(
          Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                        AuthenticationFilter.SIGNATURE_SECRET,
                        "management.operation.return")).elements());
      getMockedServletContextWithStringSigner(config);
      filter.init(config);

      HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
      Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer("http://foo:8080/bar"));

      AuthenticationToken token = new AuthenticationToken("u", "p", "invalidtype");
      token.setExpires(System.currentTimeMillis() + TOKEN_VALIDITY_SEC);
      SignerSecretProvider secretProvider =
          StringSignerSecretProviderCreator.newStringSignerSecretProvider();
      Properties secretProviderProps = new Properties();
      secretProviderProps.setProperty(
              AuthenticationFilter.SIGNATURE_SECRET, secret);
      secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
      Signer signer = new Signer(secretProvider);
      String tokenSigned = signer.sign(token.toString());

      Cookie cookie = new Cookie(AuthenticatedURL.AUTH_COOKIE, tokenSigned);
      Mockito.when(request.getCookies()).thenReturn(new Cookie[]{cookie});

      HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
      Mockito.when(response.containsHeader("WWW-Authenticate")).thenReturn(true);
      FilterChain chain = Mockito.mock(FilterChain.class);

      verifyUnauthorized(filter, request, response, chain);
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void testManagementOperation() throws Exception {
    AuthenticationFilter filter = new AuthenticationFilter();
    try {
      FilterConfig config = Mockito.mock(FilterConfig.class);
      Mockito.when(config.getInitParameter("management.operation.return")).
        thenReturn("false");
      Mockito.when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).
        thenReturn(DummyAuthenticationHandler.class.getName());
      Mockito.when(config.getInitParameterNames()).thenReturn(
        new Vector<String>(
          Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                        "management.operation.return")).elements());
      getMockedServletContextWithStringSigner(config);
      filter.init(config);

      HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
      Mockito.when(request.getRequestURL()).
        thenReturn(new StringBuffer("http://foo:8080/bar"));

      HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

      FilterChain chain = Mockito.mock(FilterChain.class);

      filter.doFilter(request, response, chain);
      Mockito.verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
      Mockito.verifyNoMoreInteractions(response);

      Mockito.reset(request);
      Mockito.reset(response);

      AuthenticationToken token = new AuthenticationToken("u", "p", "t");
      token.setExpires(System.currentTimeMillis() + TOKEN_VALIDITY_SEC);
      SignerSecretProvider secretProvider =
          StringSignerSecretProviderCreator.newStringSignerSecretProvider();
      Properties secretProviderProps = new Properties();
      secretProviderProps.setProperty(
              AuthenticationFilter.SIGNATURE_SECRET, "secret");
      secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
      Signer signer = new Signer(secretProvider);
      String tokenSigned = signer.sign(token.toString());
      Cookie cookie = new Cookie(AuthenticatedURL.AUTH_COOKIE, tokenSigned);
      Mockito.when(request.getCookies()).thenReturn(new Cookie[]{cookie});
      Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer());

      filter.doFilter(request, response, chain);

      Mockito.verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
      Mockito.verifyNoMoreInteractions(response);

    } finally {
      filter.destroy();
    }
  }

}

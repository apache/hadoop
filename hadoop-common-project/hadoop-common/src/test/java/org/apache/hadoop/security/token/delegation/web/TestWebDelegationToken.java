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
package org.apache.hadoop.security.token.delegation.web;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.ServletHolder;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.servlet.Filter;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URL;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

public class TestWebDelegationToken {
  private static final String OK_USER = "ok-user";
  private static final String FAIL_USER = "fail-user";
  private static final String FOO_USER = "foo";
  
  private Server jetty;

  public static class DummyAuthenticationHandler
      implements AuthenticationHandler {
    @Override
    public String getType() {
      return "dummy";
    }

    @Override
    public void init(Properties config) throws ServletException {
    }

    @Override
    public void destroy() {
    }

    @Override
    public boolean managementOperation(AuthenticationToken token,
        HttpServletRequest request, HttpServletResponse response)
        throws IOException, AuthenticationException {
      return false;
    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request,
        HttpServletResponse response)
        throws IOException, AuthenticationException {
      AuthenticationToken token = null;
      if (request.getParameter("authenticated") != null) {
        token = new AuthenticationToken(request.getParameter("authenticated"),
            "U", "test");
      } else {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setHeader(KerberosAuthenticator.WWW_AUTHENTICATE, "dummy");
      }
      return token;
    }
  }

  public static class DummyDelegationTokenAuthenticationHandler extends
      DelegationTokenAuthenticationHandler {
    public DummyDelegationTokenAuthenticationHandler() {
      super(new DummyAuthenticationHandler());
    }

    @Override
    public void init(Properties config) throws ServletException {
      Properties conf = new Properties(config);
      conf.setProperty(TOKEN_KIND, "token-kind");
      initTokenManager(conf);
    }
  }

  public static class AFilter extends DelegationTokenAuthenticationFilter {

    @Override
    protected Properties getConfiguration(String configPrefix,
        FilterConfig filterConfig) {
      Properties conf = new Properties();
      conf.setProperty(AUTH_TYPE,
          DummyDelegationTokenAuthenticationHandler.class.getName());
      return conf;
    }
  }

  public static class PingServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      resp.setStatus(HttpServletResponse.SC_OK);
      resp.getWriter().write("ping");
      if (req.getHeader(DelegationTokenAuthenticator.DELEGATION_TOKEN_HEADER)
          != null) {
        resp.setHeader("UsingHeader", "true");
      }
      if (req.getQueryString() != null &&
          req.getQueryString().contains(
              DelegationTokenAuthenticator.DELEGATION_PARAM + "=")) {
        resp.setHeader("UsingQueryString", "true");
      }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      Writer writer = resp.getWriter();
      writer.write("ping: ");
      IOUtils.copy(req.getReader(), writer);
      resp.setStatus(HttpServletResponse.SC_OK);
    }
  }

  protected Server createJettyServer() {
    try {
      InetAddress localhost = InetAddress.getLocalHost();
      ServerSocket ss = new ServerSocket(0, 50, localhost);
      int port = ss.getLocalPort();
      ss.close();
      jetty = new Server(0);
      jetty.getConnectors()[0].setHost("localhost");
      jetty.getConnectors()[0].setPort(port);
      return jetty;
    } catch (Exception ex) {
      throw new RuntimeException("Could not setup Jetty: " + ex.getMessage(),
          ex);
    }
  }

  protected String getJettyURL() {
    Connector c = jetty.getConnectors()[0];
    return "http://" + c.getHost() + ":" + c.getPort();
  }

  @Before
  public void setUp() throws Exception {
    // resetting hadoop security to simple
    org.apache.hadoop.conf.Configuration conf =
        new org.apache.hadoop.conf.Configuration();
    UserGroupInformation.setConfiguration(conf);

    jetty = createJettyServer();
  }

  @After
  public void cleanUp() throws Exception {
    jetty.stop();

    // resetting hadoop security to simple
    org.apache.hadoop.conf.Configuration conf =
        new org.apache.hadoop.conf.Configuration();
    UserGroupInformation.setConfiguration(conf);
  }

  protected Server getJetty() {
    return jetty;
  }

  @Test
  public void testRawHttpCalls() throws Exception {
    final Server jetty = createJettyServer();
    Context context = new Context();
    context.setContextPath("/foo");
    jetty.setHandler(context);
    context.addFilter(new FilterHolder(AFilter.class), "/*", 0);
    context.addServlet(new ServletHolder(PingServlet.class), "/bar");
    try {
      jetty.start();
      URL nonAuthURL = new URL(getJettyURL() + "/foo/bar");
      URL authURL = new URL(getJettyURL() + "/foo/bar?authenticated=foo");

      // unauthenticated access to URL
      HttpURLConnection conn = (HttpURLConnection) nonAuthURL.openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED,
          conn.getResponseCode());

      // authenticated access to URL
      conn = (HttpURLConnection) authURL.openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

      // unauthenticated access to get delegation token
      URL url = new URL(nonAuthURL.toExternalForm() + "?op=GETDELEGATIONTOKEN");
      conn = (HttpURLConnection) url.openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED,
          conn.getResponseCode());

      // authenticated access to get delegation token
      url = new URL(authURL.toExternalForm() +
          "&op=GETDELEGATIONTOKEN&renewer=foo");
      conn = (HttpURLConnection) url.openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      ObjectMapper mapper = new ObjectMapper();
      Map map = mapper.readValue(conn.getInputStream(), Map.class);
      String dt = (String) ((Map) map.get("Token")).get("urlString");
      Assert.assertNotNull(dt);

      // delegation token access to URL
      url = new URL(nonAuthURL.toExternalForm() + "?delegation=" + dt);
      conn = (HttpURLConnection) url.openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

      // delegation token and authenticated access to URL
      url = new URL(authURL.toExternalForm() + "&delegation=" + dt);
      conn = (HttpURLConnection) url.openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

      // renewew delegation token, unauthenticated access to URL
      url = new URL(nonAuthURL.toExternalForm() +
          "?op=RENEWDELEGATIONTOKEN&token=" + dt);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("PUT");
      Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED,
          conn.getResponseCode());

      // renewew delegation token, authenticated access to URL
      url = new URL(authURL.toExternalForm() +
          "&op=RENEWDELEGATIONTOKEN&token=" + dt);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("PUT");
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

      // renewew delegation token, authenticated access to URL, not renewer
      url = new URL(getJettyURL() +
          "/foo/bar?authenticated=bar&op=RENEWDELEGATIONTOKEN&token=" + dt);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("PUT");
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
          conn.getResponseCode());

      // cancel delegation token, nonauthenticated access to URL
      url = new URL(nonAuthURL.toExternalForm() +
          "?op=CANCELDELEGATIONTOKEN&token=" + dt);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("PUT");
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

      // cancel canceled delegation token, nonauthenticated access to URL
      url = new URL(nonAuthURL.toExternalForm() +
          "?op=CANCELDELEGATIONTOKEN&token=" + dt);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("PUT");
      Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
          conn.getResponseCode());

      // get new delegation token
      url = new URL(authURL.toExternalForm() +
          "&op=GETDELEGATIONTOKEN&renewer=foo");
      conn = (HttpURLConnection) url.openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      mapper = new ObjectMapper();
      map = mapper.readValue(conn.getInputStream(), Map.class);
      dt = (String) ((Map) map.get("Token")).get("urlString");
      Assert.assertNotNull(dt);

      // cancel delegation token, authenticated access to URL
      url = new URL(authURL.toExternalForm() +
          "&op=CANCELDELEGATIONTOKEN&token=" + dt);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("PUT");
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    } finally {
      jetty.stop();
    }
  }

  @Test
  public void testDelegationTokenAuthenticatorCallsWithHeader()
      throws Exception {
    testDelegationTokenAuthenticatorCalls(false);
  }

  @Test
  public void testDelegationTokenAuthenticatorCallsWithQueryString()
      throws Exception {
    testDelegationTokenAuthenticatorCalls(true);
  }


  private void testDelegationTokenAuthenticatorCalls(final boolean useQS)
      throws Exception {
    final Server jetty = createJettyServer();
    Context context = new Context();
    context.setContextPath("/foo");
    jetty.setHandler(context);
    context.addFilter(new FilterHolder(AFilter.class), "/*", 0);
    context.addServlet(new ServletHolder(PingServlet.class), "/bar");

    try {
      jetty.start();
      final URL nonAuthURL = new URL(getJettyURL() + "/foo/bar");
      URL authURL = new URL(getJettyURL() + "/foo/bar?authenticated=foo");
      URL authURL2 = new URL(getJettyURL() + "/foo/bar?authenticated=bar");

      DelegationTokenAuthenticatedURL.Token token =
          new DelegationTokenAuthenticatedURL.Token();
      final DelegationTokenAuthenticatedURL aUrl =
          new DelegationTokenAuthenticatedURL();
      aUrl.setUseQueryStringForDelegationToken(useQS);

      try {
        aUrl.getDelegationToken(nonAuthURL, token, FOO_USER);
        Assert.fail();
      } catch (Exception ex) {
        Assert.assertTrue(ex.getMessage().contains("401"));
      }

      aUrl.getDelegationToken(authURL, token, FOO_USER);
      Assert.assertNotNull(token.getDelegationToken());
      Assert.assertEquals(new Text("token-kind"),
          token.getDelegationToken().getKind());

      aUrl.renewDelegationToken(authURL, token);

      try {
        aUrl.renewDelegationToken(nonAuthURL, token);
        Assert.fail();
      } catch (Exception ex) {
        Assert.assertTrue(ex.getMessage().contains("401"));
      }

      aUrl.getDelegationToken(authURL, token, FOO_USER);

      try {
        aUrl.renewDelegationToken(authURL2, token);
        Assert.fail();
      } catch (Exception ex) {
        Assert.assertTrue(ex.getMessage().contains("403"));
      }

      aUrl.getDelegationToken(authURL, token, FOO_USER);

      aUrl.cancelDelegationToken(authURL, token);

      aUrl.getDelegationToken(authURL, token, FOO_USER);

      aUrl.cancelDelegationToken(nonAuthURL, token);

      aUrl.getDelegationToken(authURL, token, FOO_USER);

      try {
        aUrl.renewDelegationToken(nonAuthURL, token);
      } catch (Exception ex) {
        Assert.assertTrue(ex.getMessage().contains("401"));
      }

      aUrl.getDelegationToken(authURL, token, "foo");

      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      ugi.addToken(token.getDelegationToken());
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
                 @Override
                 public Void run() throws Exception {
                   HttpURLConnection conn = aUrl.openConnection(nonAuthURL, new DelegationTokenAuthenticatedURL.Token());
                   Assert.assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                   if (useQS) {
                     Assert.assertNull(conn.getHeaderField("UsingHeader"));
                     Assert.assertNotNull(conn.getHeaderField("UsingQueryString"));
                   } else {
                     Assert.assertNotNull(conn.getHeaderField("UsingHeader"));
                     Assert.assertNull(conn.getHeaderField("UsingQueryString"));
                   }
                   return null;
                 }
               });


    } finally {
      jetty.stop();
    }
  }

  private static class DummyDelegationTokenSecretManager
      extends AbstractDelegationTokenSecretManager<DelegationTokenIdentifier> {

    public DummyDelegationTokenSecretManager() {
      super(10000, 10000, 10000, 10000);
    }

    @Override
    public DelegationTokenIdentifier createIdentifier() {
      return new DelegationTokenIdentifier(new Text("fooKind"));
    }

  }

  @Test
  public void testExternalDelegationTokenSecretManager() throws Exception {
    DummyDelegationTokenSecretManager secretMgr
        = new DummyDelegationTokenSecretManager();
    final Server jetty = createJettyServer();
    Context context = new Context();
    context.setContextPath("/foo");
    jetty.setHandler(context);
    context.addFilter(new FilterHolder(AFilter.class), "/*", 0);
    context.addServlet(new ServletHolder(PingServlet.class), "/bar");
    try {
      secretMgr.startThreads();
      context.setAttribute(DelegationTokenAuthenticationFilter.
              DELEGATION_TOKEN_SECRET_MANAGER_ATTR, secretMgr);
      jetty.start();
      URL authURL = new URL(getJettyURL() + "/foo/bar?authenticated=foo");

      DelegationTokenAuthenticatedURL.Token token =
          new DelegationTokenAuthenticatedURL.Token();
      DelegationTokenAuthenticatedURL aUrl =
          new DelegationTokenAuthenticatedURL();

      aUrl.getDelegationToken(authURL, token, FOO_USER);
      Assert.assertNotNull(token.getDelegationToken());
      Assert.assertEquals(new Text("fooKind"),
          token.getDelegationToken().getKind());

    } finally {
      jetty.stop();
      secretMgr.stopThreads();
    }
  }

  public static class NoDTFilter extends AuthenticationFilter {

    @Override
    protected Properties getConfiguration(String configPrefix,
        FilterConfig filterConfig) {
      Properties conf = new Properties();
      conf.setProperty(AUTH_TYPE, PseudoAuthenticationHandler.TYPE);
      return conf;
    }
  }


  public static class NoDTHandlerDTAFilter
      extends DelegationTokenAuthenticationFilter {

    @Override
    protected Properties getConfiguration(String configPrefix,
        FilterConfig filterConfig) {
      Properties conf = new Properties();
      conf.setProperty(AUTH_TYPE, PseudoAuthenticationHandler.TYPE);
      return conf;
    }
  }

  public static class UserServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      resp.setStatus(HttpServletResponse.SC_OK);
      resp.getWriter().write(req.getUserPrincipal().getName());
    }
  }

  @Test
  public void testDelegationTokenAuthenticationURLWithNoDTFilter()
    throws Exception {
    testDelegationTokenAuthenticatedURLWithNoDT(NoDTFilter.class);
  }

  @Test
  public void testDelegationTokenAuthenticationURLWithNoDTHandler()
      throws Exception {
    testDelegationTokenAuthenticatedURLWithNoDT(NoDTHandlerDTAFilter.class);
  }

  // we are, also, implicitly testing  KerberosDelegationTokenAuthenticator
  // fallback here
  private void testDelegationTokenAuthenticatedURLWithNoDT(
      Class<? extends Filter> filterClass)  throws Exception {
    final Server jetty = createJettyServer();
    Context context = new Context();
    context.setContextPath("/foo");
    jetty.setHandler(context);
    context.addFilter(new FilterHolder(filterClass), "/*", 0);
    context.addServlet(new ServletHolder(UserServlet.class), "/bar");

    try {
      jetty.start();
      final URL url = new URL(getJettyURL() + "/foo/bar");

      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(FOO_USER);
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          DelegationTokenAuthenticatedURL.Token token =
              new DelegationTokenAuthenticatedURL.Token();
          DelegationTokenAuthenticatedURL aUrl =
              new DelegationTokenAuthenticatedURL();
          HttpURLConnection conn = aUrl.openConnection(url, token);
          Assert.assertEquals(HttpURLConnection.HTTP_OK,
              conn.getResponseCode());
          List<String> ret = IOUtils.readLines(conn.getInputStream());
          Assert.assertEquals(1, ret.size());
          Assert.assertEquals(FOO_USER, ret.get(0));

          try {
            aUrl.getDelegationToken(url, token, FOO_USER);
            Assert.fail();
          } catch (AuthenticationException ex) {
            Assert.assertTrue(ex.getMessage().contains(
                "delegation token operation"));
          }
          return null;
        }
      });
    } finally {
      jetty.stop();
    }
  }

  public static class PseudoDTAFilter
      extends DelegationTokenAuthenticationFilter {

    @Override
    protected Properties getConfiguration(String configPrefix,
        FilterConfig filterConfig) {
      Properties conf = new Properties();
      conf.setProperty(AUTH_TYPE,
          PseudoDelegationTokenAuthenticationHandler.class.getName());
      conf.setProperty(DelegationTokenAuthenticationHandler.TOKEN_KIND,
          "token-kind");
      return conf;
    }

    @Override
    protected org.apache.hadoop.conf.Configuration getProxyuserConfiguration(
        FilterConfig filterConfig) throws ServletException {
      org.apache.hadoop.conf.Configuration conf =
          new org.apache.hadoop.conf.Configuration(false);
      conf.set("proxyuser.foo.users", OK_USER);
      conf.set("proxyuser.foo.hosts", "localhost");
      return conf;
    }
  }

  @Test
  public void testFallbackToPseudoDelegationTokenAuthenticator()
      throws Exception {
    final Server jetty = createJettyServer();
    Context context = new Context();
    context.setContextPath("/foo");
    jetty.setHandler(context);
    context.addFilter(new FilterHolder(PseudoDTAFilter.class), "/*", 0);
    context.addServlet(new ServletHolder(UserServlet.class), "/bar");

    try {
      jetty.start();
      final URL url = new URL(getJettyURL() + "/foo/bar");

      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(FOO_USER);
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          DelegationTokenAuthenticatedURL.Token token =
              new DelegationTokenAuthenticatedURL.Token();
          DelegationTokenAuthenticatedURL aUrl =
              new DelegationTokenAuthenticatedURL();
          HttpURLConnection conn = aUrl.openConnection(url, token);
          Assert.assertEquals(HttpURLConnection.HTTP_OK,
              conn.getResponseCode());
          List<String> ret = IOUtils.readLines(conn.getInputStream());
          Assert.assertEquals(1, ret.size());
          Assert.assertEquals(FOO_USER, ret.get(0));

          aUrl.getDelegationToken(url, token, FOO_USER);
          Assert.assertNotNull(token.getDelegationToken());
          Assert.assertEquals(new Text("token-kind"),
              token.getDelegationToken().getKind());
          return null;
        }
      });
    } finally {
      jetty.stop();
    }
  }

  public static class KDTAFilter extends DelegationTokenAuthenticationFilter {
    static String keytabFile;

    @Override
    protected Properties getConfiguration(String configPrefix,
        FilterConfig filterConfig) {
      Properties conf = new Properties();
      conf.setProperty(AUTH_TYPE,
          KerberosDelegationTokenAuthenticationHandler.class.getName());
      conf.setProperty(KerberosAuthenticationHandler.KEYTAB, keytabFile);
      conf.setProperty(KerberosAuthenticationHandler.PRINCIPAL,
          "HTTP/localhost");
      conf.setProperty(KerberosDelegationTokenAuthenticationHandler.TOKEN_KIND,
          "token-kind");
      return conf;
    }

    @Override
    protected org.apache.hadoop.conf.Configuration getProxyuserConfiguration(
        FilterConfig filterConfig) throws ServletException {
      org.apache.hadoop.conf.Configuration conf =
          new org.apache.hadoop.conf.Configuration(false);
      conf.set("proxyuser.client.users", OK_USER);
      conf.set("proxyuser.client.hosts", "localhost");
      return conf;
    }
  }

  private static class KerberosConfiguration extends Configuration {
    private String principal;
    private String keytab;

    public KerberosConfiguration(String principal, String keytab) {
      this.principal = principal;
      this.keytab = keytab;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options = new HashMap<String, String>();
      options.put("principal", principal);
      options.put("keyTab", keytab);
      options.put("useKeyTab", "true");
      options.put("storeKey", "true");
      options.put("doNotPrompt", "true");
      options.put("useTicketCache", "true");
      options.put("renewTGT", "true");
      options.put("refreshKrb5Config", "true");
      options.put("isInitiator", "true");
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        options.put("ticketCache", ticketCache);
      }
      options.put("debug", "true");

      return new AppConfigurationEntry[]{
          new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(),
              AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
              options),};
    }
  }

  public static <T> T doAsKerberosUser(String principal, String keytab,
      final Callable<T> callable) throws Exception {
    LoginContext loginContext = null;
    try {
      Set<Principal> principals = new HashSet<Principal>();
      principals.add(new KerberosPrincipal(principal));
      Subject subject = new Subject(false, principals, new HashSet<Object>(),
          new HashSet<Object>());
      loginContext = new LoginContext("", subject, null,
          new KerberosConfiguration(principal, keytab));
      loginContext.login();
      subject = loginContext.getSubject();
      return Subject.doAs(subject, new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws Exception {
          return callable.call();
        }
      });
    } catch (PrivilegedActionException ex) {
      throw ex.getException();
    } finally {
      if (loginContext != null) {
        loginContext.logout();
      }
    }
  }

  @Test
  public void testKerberosDelegationTokenAuthenticator() throws Exception {
    testKerberosDelegationTokenAuthenticator(false);
  }

  @Test
  public void testKerberosDelegationTokenAuthenticatorWithDoAs()
      throws Exception {
    testKerberosDelegationTokenAuthenticator(true);
  }

  private void testKerberosDelegationTokenAuthenticator(
      final boolean doAs) throws Exception {
    final String doAsUser = doAs ? OK_USER : null;

    // setting hadoop security to kerberos
    org.apache.hadoop.conf.Configuration conf =
        new org.apache.hadoop.conf.Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);

    File testDir = new File("target/" + UUID.randomUUID().toString());
    Assert.assertTrue(testDir.mkdirs());
    MiniKdc kdc = new MiniKdc(MiniKdc.createConf(), testDir);
    final Server jetty = createJettyServer();
    Context context = new Context();
    context.setContextPath("/foo");
    jetty.setHandler(context);
    context.addFilter(new FilterHolder(KDTAFilter.class), "/*", 0);
    context.addServlet(new ServletHolder(UserServlet.class), "/bar");
    try {
      kdc.start();
      File keytabFile = new File(testDir, "test.keytab");
      kdc.createPrincipal(keytabFile, "client", "HTTP/localhost");
      KDTAFilter.keytabFile = keytabFile.getAbsolutePath();
      jetty.start();

      final DelegationTokenAuthenticatedURL.Token token =
          new DelegationTokenAuthenticatedURL.Token();
      final DelegationTokenAuthenticatedURL aUrl =
          new DelegationTokenAuthenticatedURL();
      final URL url = new URL(getJettyURL() + "/foo/bar");

      try {
        aUrl.getDelegationToken(url, token, FOO_USER, doAsUser);
        Assert.fail();
      } catch (AuthenticationException ex) {
        Assert.assertTrue(ex.getMessage().contains("GSSException"));
      }

      doAsKerberosUser("client", keytabFile.getAbsolutePath(),
          new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              aUrl.getDelegationToken(
                  url, token, doAs ? doAsUser : "client", doAsUser);
              Assert.assertNotNull(token.getDelegationToken());
              Assert.assertEquals(new Text("token-kind"),
                  token.getDelegationToken().getKind());
              // Make sure the token belongs to the right owner
              ByteArrayInputStream buf = new ByteArrayInputStream(
                  token.getDelegationToken().getIdentifier());
              DataInputStream dis = new DataInputStream(buf);
              DelegationTokenIdentifier id =
                  new DelegationTokenIdentifier(new Text("token-kind"));
              id.readFields(dis);
              dis.close();
              Assert.assertEquals(
                  doAs ? new Text(OK_USER) : new Text("client"), id.getOwner());
              if (doAs) {
                Assert.assertEquals(new Text("client"), id.getRealUser());
              }

              aUrl.renewDelegationToken(url, token, doAsUser);
              Assert.assertNotNull(token.getDelegationToken());

              aUrl.getDelegationToken(url, token, FOO_USER, doAsUser);
              Assert.assertNotNull(token.getDelegationToken());

              try {
                aUrl.renewDelegationToken(url, token, doAsUser);
                Assert.fail();
              } catch (Exception ex) {
                Assert.assertTrue(ex.getMessage().contains("403"));
              }

              aUrl.getDelegationToken(url, token, FOO_USER, doAsUser);

              aUrl.cancelDelegationToken(url, token, doAsUser);
              Assert.assertNull(token.getDelegationToken());

              return null;
            }
          });
    } finally {
      jetty.stop();
      kdc.stop();
    }
  }

  @Test
  public void testProxyUser() throws Exception {
    final Server jetty = createJettyServer();
    Context context = new Context();
    context.setContextPath("/foo");
    jetty.setHandler(context);
    context.addFilter(new FilterHolder(PseudoDTAFilter.class), "/*", 0);
    context.addServlet(new ServletHolder(UserServlet.class), "/bar");

    try {
      jetty.start();
      final URL url = new URL(getJettyURL() + "/foo/bar");

      // proxyuser using raw HTTP, verifying doAs is case insensitive
      String strUrl = String.format("%s?user.name=%s&doas=%s", 
          url.toExternalForm(), FOO_USER, OK_USER);
      HttpURLConnection conn = 
          (HttpURLConnection) new URL(strUrl).openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      List<String> ret = IOUtils.readLines(conn.getInputStream());
      Assert.assertEquals(1, ret.size());
      Assert.assertEquals(OK_USER, ret.get(0));
      strUrl = String.format("%s?user.name=%s&DOAS=%s", url.toExternalForm(), 
          FOO_USER, OK_USER);
      conn = (HttpURLConnection) new URL(strUrl).openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      ret = IOUtils.readLines(conn.getInputStream());
      Assert.assertEquals(1, ret.size());
      Assert.assertEquals(OK_USER, ret.get(0));

      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(FOO_USER);
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          DelegationTokenAuthenticatedURL.Token token =
              new DelegationTokenAuthenticatedURL.Token();
          DelegationTokenAuthenticatedURL aUrl =
              new DelegationTokenAuthenticatedURL();

          // proxyuser using authentication handler authentication
          HttpURLConnection conn = aUrl.openConnection(url, token, OK_USER);
          Assert.assertEquals(HttpURLConnection.HTTP_OK,
              conn.getResponseCode());
          List<String> ret = IOUtils.readLines(conn.getInputStream());
          Assert.assertEquals(1, ret.size());
          Assert.assertEquals(OK_USER, ret.get(0));

          // unauthorized proxy user using authentication handler authentication
          conn = aUrl.openConnection(url, token, FAIL_USER);
          Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
              conn.getResponseCode());

          // proxy using delegation token authentication
          aUrl.getDelegationToken(url, token, FOO_USER);

          UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
          ugi.addToken(token.getDelegationToken());
          token = new DelegationTokenAuthenticatedURL.Token();

          // requests using delegation token as auth do not honor doAs
          conn = aUrl.openConnection(url, token, OK_USER);
          Assert.assertEquals(HttpURLConnection.HTTP_OK,
              conn.getResponseCode());
          ret = IOUtils.readLines(conn.getInputStream());
          Assert.assertEquals(1, ret.size());
          Assert.assertEquals(FOO_USER, ret.get(0));

          return null;
        }
      });
    } finally {
      jetty.stop();
    }
  }


  public static class UGIServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      UserGroupInformation ugi = HttpUserGroupInformation.get();
      if (ugi != null) {
        String ret = "remoteuser=" + req.getRemoteUser() + ":ugi=" +
            ugi.getShortUserName();
        if (ugi.getAuthenticationMethod() ==
            UserGroupInformation.AuthenticationMethod.PROXY) {
          ret = "realugi=" + ugi.getRealUser().getShortUserName() + ":" + ret;
        }
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.getWriter().write(ret);
      } else {
        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    }
  }

  @Test
  public void testHttpUGI() throws Exception {
    final Server jetty = createJettyServer();
    Context context = new Context();
    context.setContextPath("/foo");
    jetty.setHandler(context);
    context.addFilter(new FilterHolder(PseudoDTAFilter.class), "/*", 0);
    context.addServlet(new ServletHolder(UGIServlet.class), "/bar");

    try {
      jetty.start();
      final URL url = new URL(getJettyURL() + "/foo/bar");

      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(FOO_USER);
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          DelegationTokenAuthenticatedURL.Token token =
              new DelegationTokenAuthenticatedURL.Token();
          DelegationTokenAuthenticatedURL aUrl =
              new DelegationTokenAuthenticatedURL();

          // user foo
          HttpURLConnection conn = aUrl.openConnection(url, token);
          Assert.assertEquals(HttpURLConnection.HTTP_OK,
              conn.getResponseCode());
          List<String> ret = IOUtils.readLines(conn.getInputStream());
          Assert.assertEquals(1, ret.size());
          Assert.assertEquals("remoteuser=" + FOO_USER+ ":ugi=" + FOO_USER, 
              ret.get(0));

          // user ok-user via proxyuser foo
          conn = aUrl.openConnection(url, token, OK_USER);
          Assert.assertEquals(HttpURLConnection.HTTP_OK,
              conn.getResponseCode());
          ret = IOUtils.readLines(conn.getInputStream());
          Assert.assertEquals(1, ret.size());
          Assert.assertEquals("realugi=" + FOO_USER +":remoteuser=" + OK_USER + 
                  ":ugi=" + OK_USER, ret.get(0));

          return null;
        }
      });
    } finally {
      jetty.stop();
    }
  }

}

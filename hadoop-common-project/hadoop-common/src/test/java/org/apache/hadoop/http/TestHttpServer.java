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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.http.HttpServer2.QuotingInputFilter.RequestQuoter;
import org.apache.hadoop.http.resource.JerseyResource;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.authorize.AccessControlList;

import org.assertj.core.api.Assertions;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.util.ajax.JSON;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class TestHttpServer extends HttpServerFunctionalTest {
  static final Logger LOG = LoggerFactory.getLogger(TestHttpServer.class);
  private static HttpServer2 server;
  private static final int MAX_THREADS = 10;

  public static class EchoMapServlet extends HttpServlet {
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {
      response.setContentType(MediaType.TEXT_PLAIN + "; " + JettyUtils.UTF_8);
      PrintWriter out = response.getWriter();
      Map<String, String[]> params = request.getParameterMap();
      SortedSet<String> keys = new TreeSet<>(params.keySet());
      for(String key: keys) {
        out.print(key);
        out.print(':');
        String[] values = params.get(key);
        if (values.length > 0) {
          out.print(values[0]);
          for(int i=1; i < values.length; ++i) {
            out.print(',');
            out.print(values[i]);
          }
        }
        out.print('\n');
      }
      out.close();
    }    
  }

  public static class EchoServlet extends HttpServlet {
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {
      response.setContentType(MediaType.TEXT_PLAIN + "; " + JettyUtils.UTF_8);
      PrintWriter out = response.getWriter();
      SortedSet<String> sortedKeys = new TreeSet<>();
      Enumeration<String> keys = request.getParameterNames();
      while(keys.hasMoreElements()) {
        sortedKeys.add(keys.nextElement());
      }
      for(String key: sortedKeys) {
        out.print(key);
        out.print(':');
        out.print(request.getParameter(key));
        out.print('\n');
      }
      out.close();
    }    
  }

  public static class HtmlContentServlet extends HttpServlet {
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {
      response.setContentType(MediaType.TEXT_HTML + "; " + JettyUtils.UTF_8);
      PrintWriter out = response.getWriter();
      out.print("hello world");
      out.close();
    }
  }

  @BeforeClass
  public static void setup() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(HttpServer2.HTTP_MAX_THREADS_KEY, MAX_THREADS);
    conf.setBoolean(
        CommonConfigurationKeysPublic.HADOOP_HTTP_METRICS_ENABLED, true);
    server = createTestServer(conf);
    server.addServlet("echo", "/echo", EchoServlet.class);
    server.addServlet("echomap", "/echomap", EchoMapServlet.class);
    server.addServlet("htmlcontent", "/htmlcontent", HtmlContentServlet.class);
    server.addServlet("longheader", "/longheader", LongHeaderServlet.class);
    server.addJerseyResourcePackage(
        JerseyResource.class.getPackage().getName(), "/jersey/*");
    server.start();
    baseUrl = getServerURL(server);
    LOG.info("HTTP server started: {}", baseUrl);
  }
  
  @AfterClass
  public static void cleanup() throws Exception {
    server.stop();
  }
  
  /** Test the maximum number of threads cannot be exceeded. */
  @Test
  public void testMaxThreads() throws Exception {
    int clientThreads = MAX_THREADS * 10;
    Executor executor = Executors.newFixedThreadPool(clientThreads);
    // Run many clients to make server reach its maximum number of threads
    final CountDownLatch ready = new CountDownLatch(clientThreads);
    final CountDownLatch start = new CountDownLatch(1);
    for (int i = 0; i < clientThreads; i++) {
      executor.execute(() -> {
        ready.countDown();
        try {
          start.await();
          assertEquals("a:b\nc:d\n",
              readOutput(new URL(baseUrl, "/echo?a=b&c=d")));
          int serverThreads = server.webServer.getThreadPool().getThreads();
          assertTrue("More threads are started than expected, Server Threads count: " +
              serverThreads, serverThreads <= MAX_THREADS);
          LOG.info("Number of threads = {} which is less or equal than the max = {}",
              serverThreads, MAX_THREADS);
        } catch (Exception e) {
          // do nothing
        }
      });
    }
    // Start the client threads when they are all ready
    ready.await();
    start.countDown();
  }

  /**
   * Test that the number of acceptors and selectors can be configured by
   * trying to configure more of them than would be allowed based on the
   * maximum thread count.
   */
  @Test
  public void testAcceptorSelectorConfigurability() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(HttpServer2.HTTP_MAX_THREADS_KEY, MAX_THREADS);
    conf.setInt(HttpServer2.HTTP_ACCEPTOR_COUNT_KEY, MAX_THREADS - 2);
    conf.setInt(HttpServer2.HTTP_SELECTOR_COUNT_KEY, MAX_THREADS - 2);
    HttpServer2 badserver = createTestServer(conf);
    try {
      badserver.start();
      // Should not succeed
      fail();
    } catch (IOException ioe) {
      assertTrue(ioe.getCause() instanceof IllegalStateException);
    }
  }
  
  @Test
  public void testEcho() throws Exception {
    assertEquals("a:b\nc:d\n", 
                 readOutput(new URL(baseUrl, "/echo?a=b&c=d")));
    assertEquals("a:b\nc&lt;:d\ne:&gt;\n", 
                 readOutput(new URL(baseUrl, "/echo?a=b&c<=d&e=>")));    
  }
  
  /** Test the echo map servlet that uses getParameterMap. */
  @Test
  public void testEchoMap() throws Exception {
    assertEquals("a:b\nc:d\n", 
                 readOutput(new URL(baseUrl, "/echomap?a=b&c=d")));
    assertEquals("a:b,&gt;\nc&lt;:d\n", 
                 readOutput(new URL(baseUrl, "/echomap?a=b&c<=d&a=>")));
  }

  @Test
  public void testLongHeader() throws Exception {
    URL url = new URL(baseUrl, "/longheader");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    testLongHeader(conn);
  }

  @Test public void testContentTypes() throws Exception {
    // Static CSS files should have text/css
    URL cssUrl = new URL(baseUrl, "/static/test.css");
    HttpURLConnection conn = (HttpURLConnection)cssUrl.openConnection();
    conn.connect();
    assertEquals(200, conn.getResponseCode());
    assertEquals("text/css", conn.getContentType());

    // Servlets should have text/plain with proper encoding by default
    URL servletUrl = new URL(baseUrl, "/echo?a=b");
    conn = (HttpURLConnection)servletUrl.openConnection();
    conn.connect();
    assertEquals(200, conn.getResponseCode());
    assertEquals(MediaType.TEXT_PLAIN + ";" + JettyUtils.UTF_8,
        conn.getContentType());

    // We should ignore parameters for mime types - ie a parameter
    // ending in .css should not change mime type
    servletUrl = new URL(baseUrl, "/echo?a=b.css");
    conn = (HttpURLConnection)servletUrl.openConnection();
    conn.connect();
    assertEquals(200, conn.getResponseCode());
    assertEquals(MediaType.TEXT_PLAIN + ";" + JettyUtils.UTF_8,
        conn.getContentType());

    // Servlets that specify text/html should get that content type
    servletUrl = new URL(baseUrl, "/htmlcontent");
    conn = (HttpURLConnection)servletUrl.openConnection();
    conn.connect();
    assertEquals(200, conn.getResponseCode());
    assertEquals(MediaType.TEXT_HTML + ";" + JettyUtils.UTF_8,
        conn.getContentType());
  }

  @Test
  public void testHttpServer2Metrics() throws Exception {
    final HttpServer2Metrics metrics = server.getMetrics();
    final int before = metrics.responses2xx();
    final URL servletUrl = new URL(baseUrl, "/echo?echo");
    final HttpURLConnection conn =
        (HttpURLConnection)servletUrl.openConnection();
    conn.connect();
    Assertions.assertThat(conn.getResponseCode()).isEqualTo(200);
    final int after = metrics.responses2xx();
    Assertions.assertThat(after).isGreaterThan(before);
  }

  /**
   * Jetty StatisticsHandler must be inserted via Server#insertHandler
   * instead of Server#setHandler. The server fails to start if
   * the handler is added by setHandler.
   */
  @Test
  public void testSetStatisticsHandler() throws Exception {
    final Configuration conf = new Configuration();
    // skip insert
    conf.setBoolean(
        CommonConfigurationKeysPublic.HADOOP_HTTP_METRICS_ENABLED, false);
    final HttpServer2 testServer = createTestServer(conf);
    testServer.webServer.setHandler(new StatisticsHandler());
    try {
      testServer.start();
      fail("IOException should be thrown.");
    } catch (IOException ignore) {
    }
  }

  @Test
  public void testHttpResponseContainsXFrameOptions() throws Exception {
    validateXFrameOption(HttpServer2.XFrameOption.SAMEORIGIN);
  }

  @Test
  public void testHttpResponseContainsDeny() throws Exception {
    validateXFrameOption(HttpServer2.XFrameOption.DENY);
  }

  @Test
  public void testHttpResponseContainsAllowFrom() throws Exception {
    validateXFrameOption(HttpServer2.XFrameOption.ALLOWFROM);
  }

  private void validateXFrameOption(HttpServer2.XFrameOption option) throws
      Exception {
    Configuration conf = new Configuration();
    boolean xFrameEnabled = true;
    HttpServer2 httpServer = createServer(xFrameEnabled,
        option.toString(), conf);
    try {
      HttpURLConnection conn = getHttpURLConnection(httpServer);
      String xfoHeader = conn.getHeaderField("X-FRAME-OPTIONS");
      assertNotNull("X-FRAME-OPTIONS is absent in the header", xfoHeader);
      assertTrue(xfoHeader.endsWith(option.toString()));
    } finally {
      httpServer.stop();
    }
  }

  @Test
  public void testHttpResponseDoesNotContainXFrameOptions() throws Exception {
    Configuration conf = new Configuration();
    boolean xFrameEnabled = false;
    HttpServer2 httpServer = createServer(xFrameEnabled,
        HttpServer2.XFrameOption.SAMEORIGIN.toString(), conf);
    try {
      HttpURLConnection conn = getHttpURLConnection(httpServer);
      String xfoHeader = conn.getHeaderField("X-FRAME-OPTIONS");
      assertNull("Unexpected X-FRAME-OPTIONS in header", xfoHeader);
    } finally {
      httpServer.stop();
    }
  }

  private HttpURLConnection getHttpURLConnection(HttpServer2 httpServer)
      throws IOException {
    httpServer.start();
    URL newURL = getServerURL(httpServer);
    URL url = new URL(newURL, "");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.connect();
    return conn;
  }

  @Test
  public void testHttpResponseInvalidValueType() {
    Configuration conf = new Configuration();
    boolean xFrameEnabled = true;
    assertThrows(IllegalArgumentException.class, () ->
        createServer(xFrameEnabled, "Hadoop", conf));
  }


  /**
   * Dummy filter that mimics as an authentication filter. Obtains user identity
   * from the request parameter user.name. Wraps around the request so that
   * request.getRemoteUser() returns the user identity.
   * 
   */
  public static class DummyServletFilter implements Filter {
    @Override
    public void destroy() { }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
        FilterChain filterChain) throws IOException, ServletException {
      final String userName = request.getParameter("user.name");
      ServletRequest requestModified =
        new HttpServletRequestWrapper((HttpServletRequest) request) {
        @Override
        public String getRemoteUser() {
          return userName;
        }
      };
      filterChain.doFilter(requestModified, response);
    }

    @Override
    public void init(FilterConfig arg0) throws ServletException { }
  }

  /**
   * FilterInitializer that initialized the DummyFilter.
   *
   */
  public static class DummyFilterInitializer extends FilterInitializer {
    public DummyFilterInitializer() {
    }

    @Override
    public void initFilter(FilterContainer container, Configuration conf) {
      container.addFilter("DummyFilter", DummyServletFilter.class.getName(), null);
    }
  }

  /**
   * Access a URL and get the corresponding return Http status code. The URL
   * will be accessed as the passed user, by sending user.name request
   * parameter.
   * 
   * @param urlString web url.
   * @param userName userName.
   * @return http status code.
   * @throws IOException an I/O exception of some sort has occurred.
   */
  static int getHttpStatusCode(String urlString, String userName)
      throws IOException {
    URL url = new URL(urlString + "?user.name=" + userName);
    System.out.println("Accessing " + url + " as user " + userName);
    HttpURLConnection connection = (HttpURLConnection)url.openConnection();
    connection.connect();
    return connection.getResponseCode();
  }

  /**
   * Custom user->group mapping service.
   */
  public static class MyGroupsProvider extends ShellBasedUnixGroupsMapping {
    private static Map<String, List<String>> mapping = new HashMap<>();

    static void clearMapping() {
      mapping.clear();
    }

    @Override
    public List<String> getGroups(String user) throws IOException {
      return mapping.get(user);
    }

    @Override
    public Set<String> getGroupsSet(String user) throws IOException {
      return new HashSet<>(mapping.get(user));
    }
  }

  /**
   * Verify the access for /logs, /stacks, /conf, and /logLevel
   * servlets, when authentication filters are set, but authorization is not
   * enabled.
   * @throws Exception if there is an error during, an exception will be thrown.
   */
  @Test
  public void testDisabledAuthorizationOfDefaultServlets() throws Exception {

    Configuration conf = new Configuration();

    // Authorization is disabled by default
    conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        DummyFilterInitializer.class.getName());
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        MyGroupsProvider.class.getName());
    Groups.getUserToGroupsMappingService(conf);
    MyGroupsProvider.clearMapping();
    MyGroupsProvider.mapping.put("userA", Collections.singletonList("groupA"));
    MyGroupsProvider.mapping.put("userB", Collections.singletonList("groupB"));

    HttpServer2 myServer = new HttpServer2.Builder().setName("test")
        .addEndpoint(new URI("http://localhost:0")).setFindPort(true).build();
    myServer.setAttribute(HttpServer2.CONF_CONTEXT_ATTRIBUTE, conf);
    myServer.start();
    String serverURL = "http://" +
        NetUtils.getHostPortString(Objects.requireNonNull(myServer.getConnectorAddress(0))) + "/";
    for (String servlet : new String[]{"conf", "logs", "stacks", "logLevel"}) {
      for (String user : new String[] { "userA", "userB" }) {
        assertEquals(HttpURLConnection.HTTP_OK, getHttpStatusCode(serverURL
            + servlet, user));
      }
    }
    myServer.stop();
  }

  /**
   * Verify the administrator access for /logs, /stacks, /conf, and /logLevel
   * servlets.
   * 
   * @throws Exception if there is an error during, an exception will be thrown.
   */
  @Test
  public void testAuthorizationOfDefaultServlets() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        true);
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN,
        true);
    conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        DummyFilterInitializer.class.getName());

    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        MyGroupsProvider.class.getName());
    Groups.getUserToGroupsMappingService(conf);
    MyGroupsProvider.clearMapping();
    MyGroupsProvider.mapping.put("userA", Collections.singletonList("groupA"));
    MyGroupsProvider.mapping.put("userB", Collections.singletonList("groupB"));
    MyGroupsProvider.mapping.put("userC", Collections.singletonList("groupC"));
    MyGroupsProvider.mapping.put("userD", Collections.singletonList("groupD"));
    MyGroupsProvider.mapping.put("userE", Collections.singletonList("groupE"));

    HttpServer2 myServer = new HttpServer2.Builder().setName("test")
        .addEndpoint(new URI("http://localhost:0")).setFindPort(true).setConf(conf)
        .setACL(new AccessControlList("userA,userB groupC,groupD")).build();
    myServer.setAttribute(HttpServer2.CONF_CONTEXT_ATTRIBUTE, conf);
    myServer.start();

    String serverURL = "http://" +
        NetUtils.getHostPortString(Objects.requireNonNull(myServer.getConnectorAddress(0))) + "/";
    for (String servlet : new String[]{"conf", "logs", "stacks", "logLevel"}) {
      for (String user : new String[]{"userA", "userB", "userC", "userD"}) {
        assertEquals(HttpURLConnection.HTTP_OK, getHttpStatusCode(serverURL
            + servlet, user));
      }
      assertEquals(HttpURLConnection.HTTP_FORBIDDEN, getHttpStatusCode(
          serverURL + servlet, "userE"));
    }
    myServer.stop();
  }
  
  @Test
  public void testRequestQuoterWithNull() {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.doReturn(null).when(request).getParameterValues("dummy");
    RequestQuoter requestQuoter = new RequestQuoter(request);
    String[] parameterValues = requestQuoter.getParameterValues("dummy");
    Assert.assertNull(
        "It should return null " + "when there are no values for the parameter",
        parameterValues);
  }

  @Test
  public void testRequestQuoterWithNotNull() {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    String[] values = new String[] { "abc", "def" };
    Mockito.doReturn(values).when(request).getParameterValues("dummy");
    RequestQuoter requestQuoter = new RequestQuoter(request);
    String[] parameterValues = requestQuoter.getParameterValues("dummy");
    Assert.assertArrayEquals("It should return Parameter Values", values, parameterValues);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> parse(String jsonString) {
    return (Map<String, Object>) JSON.parse(jsonString);
  }

  @Test public void testJersey() throws Exception {
    LOG.info("BEGIN testJersey()");
    final String js = readOutput(new URL(baseUrl, "/jersey/foo?op=bar"));
    final Map<String, Object> m = parse(js);
    LOG.info("m={}", m);
    assertEquals("foo", m.get(JerseyResource.PATH));
    assertEquals("bar", m.get(JerseyResource.OP));
    LOG.info("END testJersey()");
  }

  @Test
  public void testHasAdministratorAccess() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false);
    ServletContext context = Mockito.mock(ServletContext.class);
    Mockito.when(context.getAttribute(HttpServer2.CONF_CONTEXT_ATTRIBUTE)).thenReturn(conf);
    Mockito.when(context.getAttribute(HttpServer2.ADMINS_ACL)).thenReturn(null);
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRemoteUser()).thenReturn(null);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    //authorization OFF
    Assert.assertTrue(HttpServer2.hasAdministratorAccess(context, request, response));

    //authorization ON & user NULL
    response = Mockito.mock(HttpServletResponse.class);
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, true);
    Assert.assertFalse(HttpServer2.hasAdministratorAccess(context, request, response));
    Mockito.verify(response).sendError(Mockito.eq(HttpServletResponse.SC_FORBIDDEN), Mockito.anyString());

    //authorization ON & user NOT NULL & ACLs NULL
    response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getRemoteUser()).thenReturn("foo");
    Assert.assertTrue(HttpServer2.hasAdministratorAccess(context, request, response));

    //authorization ON & user NOT NULL & ACLs NOT NULL & user not in ACLs
    response = Mockito.mock(HttpServletResponse.class);
    AccessControlList acls = Mockito.mock(AccessControlList.class);
    Mockito.when(acls.isUserAllowed(Mockito.any())).thenReturn(false);
    Mockito.when(context.getAttribute(HttpServer2.ADMINS_ACL)).thenReturn(acls);
    Assert.assertFalse(HttpServer2.hasAdministratorAccess(context, request, response));
    Mockito.verify(response).sendError(Mockito.eq(HttpServletResponse.SC_FORBIDDEN), Mockito.anyString());

    //authorization ON & user NOT NULL & ACLs NOT NULL & user in ACLs
    response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(acls.isUserAllowed(Mockito.any())).thenReturn(true);
    Mockito.when(context.getAttribute(HttpServer2.ADMINS_ACL)).thenReturn(acls);
    Assert.assertTrue(HttpServer2.hasAdministratorAccess(context, request, response));

  }

  @Test
  public void testRequiresAuthorizationAccess() throws Exception {
    Configuration conf = new Configuration();
    ServletContext context = Mockito.mock(ServletContext.class);
    Mockito.when(context.getAttribute(HttpServer2.CONF_CONTEXT_ATTRIBUTE)).thenReturn(conf);
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    //requires admin access to instrumentation, FALSE by default
    Assert.assertTrue(HttpServer2.isInstrumentationAccessAllowed(context, request, response));

    //requires admin access to instrumentation, TRUE
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN, true);
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, true);
    AccessControlList acls = Mockito.mock(AccessControlList.class);
    Mockito.when(acls.isUserAllowed(Mockito.any())).thenReturn(false);
    Mockito.when(context.getAttribute(HttpServer2.ADMINS_ACL)).thenReturn(acls);
    Assert.assertFalse(HttpServer2.isInstrumentationAccessAllowed(context, request, response));
  }

  @Test public void testBindAddress() throws Exception {
    checkBindAddress("localhost", 0, false).stop();
    // hang onto this one for a bit more testing
    HttpServer2 myServer = checkBindAddress("localhost", 0, false);
    HttpServer2 myServer2 = null;
    try { 
      int port = Objects.requireNonNull(myServer.getConnectorAddress(0)).getPort();
      // it's already in use, true = expect a higher port
      myServer2 = checkBindAddress("localhost", port, true);
      // try to reuse the port
      port = Objects.requireNonNull(myServer2.getConnectorAddress(0)).getPort();
      myServer2.stop();
      assertNull(myServer2.getConnectorAddress(0)); // not bound
      myServer2.openListeners();
      int connectorPort = Objects.requireNonNull(myServer2.getConnectorAddress(0)).getPort();
      assertEquals(port, connectorPort); // expect same port
    } finally {
      myServer.stop();
      if (myServer2 != null) {
        myServer2.stop();
      }
    }
  }
  
  private HttpServer2 checkBindAddress(String host, int port, boolean findPort)
      throws Exception {
    HttpServer2 server = createServer(host, port);
    try {
      // not bound, ephemeral should return requested port (0 for ephemeral)
      List<ServerConnector> listeners = server.getListeners();
      ServerConnector listener = listeners.get(0);

      assertEquals(port, listener.getPort());
      // verify hostname is what was given
      server.openListeners();
      assertEquals(host, Objects.requireNonNull(server.getConnectorAddress(0)).getHostName());

      int boundPort = Objects.requireNonNull(server.getConnectorAddress(0)).getPort();
      if (port == 0) {
        assertTrue(boundPort != 0); // ephemeral should now return bound port
      } else if (findPort) {
        assertTrue(boundPort > port);
      }
    } catch (Exception e) {
      server.stop();
      throw e;
    }
    return server;
  }

  @Test
  public void testNoCacheHeader() throws Exception {
    URL url = new URL(baseUrl, "/echo?a=b&c=d");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    assertEquals("no-cache", conn.getHeaderField("Cache-Control"));
    assertEquals("no-cache", conn.getHeaderField("Pragma"));
    assertNotNull(conn.getHeaderField("Expires"));
    assertNotNull(conn.getHeaderField("Date"));
    assertEquals(conn.getHeaderField("Expires"), conn.getHeaderField("Date"));
  }

  private static void stopHttpServer(HttpServer2 server) throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testPortRanges() throws Exception {
    Configuration conf = new Configuration();
    int port =  ServerSocketUtil.waitForPort(49000, 60);
    int endPort = 49500;
    conf.set("abc", "49000-49500");
    HttpServer2.Builder builder = new HttpServer2.Builder()
        .setName("test").setConf(new Configuration()).setFindPort(false);
    IntegerRanges ranges = conf.getRange("abc", "");
    int startPort = 0;
    if (ranges != null && !ranges.isEmpty()) {
       startPort = ranges.getRangeStart();
       builder.setPortRanges(ranges);
    }
    builder.addEndpoint(URI.create("http://localhost:" + startPort));
    HttpServer2 myServer = builder.build();
    HttpServer2 myServer2 = null;
    try {
      myServer.start();
      assertEquals(port, Objects.requireNonNull(myServer.getConnectorAddress(0)).getPort());
      myServer2 = builder.build();
      myServer2.start();
      assertTrue(Objects.requireNonNull(myServer2.getConnectorAddress(0)).getPort() > port &&
          Objects.requireNonNull(myServer2.getConnectorAddress(0)).getPort() <= endPort);
    } finally {
      stopHttpServer(myServer);
      stopHttpServer(myServer2);
    }
  }

  @Test
  public void testBacklogSize() throws Exception
  {
    final int backlogSize = 2048;
    Configuration conf = new Configuration();
    conf.setInt(HttpServer2.HTTP_SOCKET_BACKLOG_SIZE_KEY, backlogSize);
    HttpServer2 srv = createServer("test", conf);
    List<ServerConnector> listeners = srv.getListeners();
    ServerConnector listener = listeners.get(0);
    assertEquals(backlogSize, listener.getAcceptQueueSize());
  }

  @Test
  public void testBacklogSize2() throws Exception
  {
    Configuration conf = new Configuration();
    HttpServer2 srv = createServer("test", conf);
    List<ServerConnector> listeners = srv.getListeners();
    ServerConnector listener = listeners.get(0);
    assertEquals(500, listener.getAcceptQueueSize());
  }

  @Test
  public void testIdleTimeout() throws Exception {
    final int idleTimeout = 1000;
    Configuration conf = new Configuration();
    conf.setInt(HttpServer2.HTTP_IDLE_TIMEOUT_MS_KEY, idleTimeout);
    HttpServer2 srv = createServer("test", conf);
    Field f = HttpServer2.class.getDeclaredField("listeners");
    f.setAccessible(true);
    List<?> listeners = (List<?>) f.get(srv);
    ServerConnector listener = (ServerConnector)listeners.get(0);
    assertEquals(idleTimeout, listener.getIdleTimeout());
  }

  @Test
  public void testHttpResponseDefaultHeaders() throws Exception {
    Configuration conf = new Configuration();
    HttpServer2  httpServer = createTestServer(conf);
    try {
      HttpURLConnection conn = getHttpURLConnection(httpServer);
      assertEquals(HttpServer2.X_XSS_PROTECTION.split(":")[1],
              conn.getHeaderField(
              HttpServer2.X_XSS_PROTECTION.split(":")[0]));
      assertEquals(HttpServer2.X_CONTENT_TYPE_OPTIONS.split(":")[1],
              conn.getHeaderField(
              HttpServer2.X_CONTENT_TYPE_OPTIONS.split(":")[0]));
    } finally {
      httpServer.stop();
    }
  }

  @Test
  public void testHttpResponseOverrideDefaultHeaders() throws Exception {
    Configuration conf = new Configuration();
    conf.set(HttpServer2.HTTP_HEADER_PREFIX+
            HttpServer2.X_XSS_PROTECTION.split(":")[0], "customXssValue");
    HttpServer2  httpServer = createTestServer(conf);
    try {
      HttpURLConnection conn = getHttpURLConnection(httpServer);
      assertEquals("customXssValue",
              conn.getHeaderField(
              HttpServer2.X_XSS_PROTECTION.split(":")[0])
      );
      assertEquals(HttpServer2.X_CONTENT_TYPE_OPTIONS.split(":")[1],
              conn.getHeaderField(
              HttpServer2.X_CONTENT_TYPE_OPTIONS.split(":")[0])
      );
    } finally {
      httpServer.stop();
    }
  }

  @Test
  public void testHttpResponseCustomHeaders() throws Exception {
    Configuration conf = new Configuration();
    String key = "customKey";
    String value = "customValue";
    conf.set(HttpServer2.HTTP_HEADER_PREFIX+key, value);
    HttpServer2  httpServer = createTestServer(conf);
    try {
      HttpURLConnection conn = getHttpURLConnection(httpServer);
      assertEquals(HttpServer2.X_XSS_PROTECTION.split(":")[1],
              conn.getHeaderField(
              HttpServer2.X_XSS_PROTECTION.split(":")[0]));
      assertEquals(HttpServer2.X_CONTENT_TYPE_OPTIONS.split(":")[1],
              conn.getHeaderField(
              HttpServer2.X_CONTENT_TYPE_OPTIONS.split(":")[0]));
      assertEquals(value, conn.getHeaderField(
              key));
    } finally {
      httpServer.stop();
    }
  }

}

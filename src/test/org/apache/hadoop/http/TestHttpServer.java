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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHttpServer {
  private HttpServer server;
  private URL baseUrl;
  
  @SuppressWarnings("serial")
  public static class EchoMapServlet extends HttpServlet {
    @SuppressWarnings("unchecked")
    @Override
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      PrintStream out = new PrintStream(response.getOutputStream());
      Map<String, String[]> params = request.getParameterMap();
      SortedSet<String> keys = new TreeSet(params.keySet());
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

  @SuppressWarnings("serial")
  public static class EchoServlet extends HttpServlet {
    @SuppressWarnings("unchecked")
    @Override
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      PrintStream out = new PrintStream(response.getOutputStream());
      SortedSet<String> sortedKeys = new TreeSet();
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

  private String readOutput(URL url) throws IOException {
    StringBuilder out = new StringBuilder();
    InputStream in = url.openConnection().getInputStream();
    byte[] buffer = new byte[64 * 1024];
    int len = in.read(buffer);
    while (len > 0) {
      out.append(new String(buffer, 0, len));
      len = in.read(buffer);
    }
    return out.toString();
  }
  
  @Before public void setup() throws Exception {
    new File(System.getProperty("build.webapps", "build/webapps") + "/test"
             ).mkdirs();
    server = new HttpServer("test", "0.0.0.0", 0, true);
    server.addServlet("echo", "/echo", EchoServlet.class);
    server.addServlet("echomap", "/echomap", EchoMapServlet.class);
    server.start();
    int port = server.getPort();
    baseUrl = new URL("http://localhost:" + port + "/");
  }
  
  @After public void cleanup() throws Exception {
    server.stop();
  }

  @Test public void testEcho() throws Exception {
    assertEquals("a:b\nc:d\n", 
                 readOutput(new URL(baseUrl, "/echo?a=b&c=d")));
    assertEquals("a:b\nc&lt;:d\ne:&gt;\n", 
                 readOutput(new URL(baseUrl, "/echo?a=b&c<=d&e=>")));    
  }
  
  /** Test the echo map servlet that uses getParameterMap. */
  @Test public void testEchoMap() throws Exception {
    assertEquals("a:b\nc:d\n", 
                 readOutput(new URL(baseUrl, "/echomap?a=b&c=d")));
    assertEquals("a:b,&gt;\nc&lt;:d\n", 
                 readOutput(new URL(baseUrl, "/echomap?a=b&c<=d&a=>")));
  }

  /**
   * Dummy filter that mimics as an authentication filter. Obtains user identity
   * from the request parameter user.name. Wraps around the request so that
   * request.getRemoteUser() returns the user identity.
   * 
   */
  public static class DummyServletFilter implements Filter {

    private static final Log LOG = LogFactory.getLog(
        DummyServletFilter.class);
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
   * @param urlstring
   * @param userName
   * @return
   * @throws IOException
   */
  static int getHttpStatusCode(String urlstring, String userName)
      throws IOException {
    URL url = new URL(urlstring + "?user.name=" + userName);
    System.out.println("Accessing " + url + " as user " + userName);
    HttpURLConnection connection = (HttpURLConnection)url.openConnection();
    connection.connect();
    return connection.getResponseCode();
  }

  /**
   * Custom user->group mapping service.
   */
  public static class MyGroupsProvider extends ShellBasedUnixGroupsMapping {
    static Map<String, List<String>> mapping = new HashMap<String, List<String>>();

    static void clearMapping() {
      mapping.clear();
    }

    @Override
    public List<String> getGroups(String user) throws IOException {
      return mapping.get(user);
    }
  }

  /**
   * Verify the access for /logs, /stacks, /conf, /logLevel and /metrics
   * servlets, when authentication filters are set, but authorization is not
   * enabled.
   * @throws Exception 
   */
  @Test
  public void testDisabledAuthorizationOfDefaultServlets() throws Exception {

    Configuration conf = new Configuration();

    // Authorization is disabled by default
    conf.set(HttpServer.FILTER_INITIALIZER_PROPERTY,
        DummyFilterInitializer.class.getName());
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        MyGroupsProvider.class.getName());
    Groups.getUserToGroupsMappingService(conf);
    MyGroupsProvider.clearMapping();
    MyGroupsProvider.mapping.put("userA", Arrays.asList("groupA"));
    MyGroupsProvider.mapping.put("userB", Arrays.asList("groupB"));

    HttpServer myServer = new HttpServer("test", "0.0.0.0", 0, true, conf);
    myServer.setAttribute(HttpServer.CONF_CONTEXT_ATTRIBUTE, conf);
    myServer.start();
    int port = myServer.getPort();
    String serverURL = "http://localhost:" + port + "/";
    for (String servlet : new String[] { "logs", "stacks", "logLevel" }) {
      for (String user : new String[] { "userA", "userB" }) {
        assertEquals(HttpURLConnection.HTTP_OK, getHttpStatusCode(serverURL
            + servlet, user));
      }
    }
    myServer.stop();
  }

  /**
   * Verify the administrator access for /logs, /stacks, /conf, /logLevel and
   * /metrics servlets.
   * 
   * @throws Exception
   */
  @Test
  public void testAuthorizationOfDefaultServlets() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        true);
    conf.set(HttpServer.FILTER_INITIALIZER_PROPERTY,
        DummyFilterInitializer.class.getName());

    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        MyGroupsProvider.class.getName());
    Groups.getUserToGroupsMappingService(conf);
    MyGroupsProvider.clearMapping();
    MyGroupsProvider.mapping.put("userA", Arrays.asList("groupA"));
    MyGroupsProvider.mapping.put("userB", Arrays.asList("groupB"));
    MyGroupsProvider.mapping.put("userC", Arrays.asList("groupC"));
    MyGroupsProvider.mapping.put("userD", Arrays.asList("groupD"));
    MyGroupsProvider.mapping.put("userE", Arrays.asList("groupE"));

    HttpServer myServer = new HttpServer("test", "0.0.0.0", 0, true, conf,
        new AccessControlList("userA,userB groupC,groupD"));
    myServer.setAttribute(HttpServer.CONF_CONTEXT_ATTRIBUTE, conf);
    myServer.start();
    int port = myServer.getPort();
    String serverURL = "http://localhost:" + port + "/";
    for (String servlet : new String[] { "logs", "stacks", "logLevel" }) {
      for (String user : new String[] { "userA", "userB", "userC", "userD" }) {
        assertEquals(HttpURLConnection.HTTP_OK, getHttpStatusCode(serverURL
            + servlet, user));
      }
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, getHttpStatusCode(
          serverURL + servlet, "userE"));
    }
    myServer.stop();
  }
}

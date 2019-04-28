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

package org.apache.hadoop.yarn.server.webproxy.amfilter;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.util.Set;
import java.util.HashSet;
import java.util.Enumeration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.FilterConfig;
import javax.servlet.FilterChain;
import javax.servlet.Filter;
import javax.servlet.ServletContext;
import javax.servlet.ServletResponse;
import javax.servlet.ServletRequest;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.base.Supplier;
import org.apache.hadoop.http.TestHttpServer;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.server.webproxy.ProxyUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.glassfish.grizzly.servlet.HttpServletResponseImpl;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test AmIpFilter. Requests to a no declared hosts should has way through
 * proxy. Another requests can be filtered with (without) user name.
 * 
 */
public class TestAmFilter {

  private String proxyHost = "localhost";
  private String proxyUri = "http://bogus";
  private String doFilterRequest;
  private AmIpServletRequestWrapper servletWrapper;

  private class TestAmIpFilter extends AmIpFilter {

    private Set<String> proxyAddresses = null;

    protected Set<String> getProxyAddresses() {
      if (proxyAddresses == null) {
        proxyAddresses = new HashSet<String>();
      }
      proxyAddresses.add(proxyHost);
      return proxyAddresses;
    }
  }

  private static class DummyFilterConfig implements FilterConfig {
    final Map<String, String> map;

    DummyFilterConfig(Map<String, String> map) {
      this.map = map;
    }

    @Override
    public String getFilterName() {
      return "dummy";
    }

    @Override
    public String getInitParameter(String arg0) {
      return map.get(arg0);
    }

    @Override
    public Enumeration<String> getInitParameterNames() {
      return Collections.enumeration(map.keySet());
    }

    @Override
    public ServletContext getServletContext() {
      return null;
    }
  }

  @Test(timeout = 5000)
  @SuppressWarnings("deprecation")
  public void filterNullCookies() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

    Mockito.when(request.getCookies()).thenReturn(null);
    Mockito.when(request.getRemoteAddr()).thenReturn(proxyHost);

    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    final AtomicBoolean invoked = new AtomicBoolean();

    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest,
          ServletResponse servletResponse) throws IOException, ServletException {
        invoked.set(true);
      }
    };

    Map<String, String> params = new HashMap<String, String>();
    params.put(AmIpFilter.PROXY_HOST, proxyHost);
    params.put(AmIpFilter.PROXY_URI_BASE, proxyUri);
    FilterConfig conf = new DummyFilterConfig(params);
    Filter filter = new TestAmIpFilter();
    filter.init(conf);
    filter.doFilter(request, response, chain);
    assertTrue(invoked.get());
    filter.destroy();
  }

  @Test
  public void testFindRedirectUrl() throws Exception {
    final String rm1 = "rm1";
    final String rm2 = "rm2";
    // generate a valid URL
    final String rm1Url = startHttpServer();
    // invalid url
    final String rm2Url = "host2:8088";

    TestAmIpFilter filter = new TestAmIpFilter();
    TestAmIpFilter spy = Mockito.spy(filter);
    // make sure findRedirectUrl() go to HA branch
    spy.proxyUriBases = new HashMap<>();
    spy.proxyUriBases.put(rm1, rm1Url);
    spy.proxyUriBases.put(rm2, rm2Url);
    spy.rmUrls = new String[] { rm1, rm2 };

    assertThat(spy.findRedirectUrl()).isEqualTo(rm1Url);
  }

  private String startHttpServer() throws Exception {
    Server server = new Server(0);
    ((QueuedThreadPool)server.getThreadPool()).setMaxThreads(10);
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/foo");
    server.setHandler(context);
    String servletPath = "/bar";
    context.addServlet(new ServletHolder(TestHttpServer.EchoServlet.class),
        servletPath);
    ((ServerConnector)server.getConnectors()[0]).setHost("localhost");
    server.start();
    System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
    return server.getURI().toString() + servletPath;
  }

  @Test(timeout = 2000)
  public void testProxyUpdate() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put(AmIpFilter.PROXY_HOSTS, proxyHost);
    params.put(AmIpFilter.PROXY_URI_BASES, proxyUri);

    FilterConfig conf = new DummyFilterConfig(params);
    AmIpFilter filter = new AmIpFilter();
    int updateInterval = 1000;
    AmIpFilter.setUpdateInterval(updateInterval);
    filter.init(conf);
    filter.getProxyAddresses();

    // check that the configuration was applied
    assertTrue(filter.getProxyAddresses().contains("127.0.0.1"));

    // change proxy configurations
    params = new HashMap<>();
    params.put(AmIpFilter.PROXY_HOSTS, "unknownhost");
    params.put(AmIpFilter.PROXY_URI_BASES, proxyUri);
    conf = new DummyFilterConfig(params);
    filter.init(conf);

    // configurations shouldn't be updated now
    assertFalse(filter.getProxyAddresses().isEmpty());
    // waiting for configuration update
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          return filter.getProxyAddresses().isEmpty();
        } catch (ServletException e) {
          return true;
        }
      }
    }, 500, updateInterval);
  }

  /**
   * Test AmIpFilter
   */
  @Test(timeout = 10000)
  @SuppressWarnings("deprecation")
  public void testFilter() throws Exception {
    Map<String, String> params = new HashMap<String, String>();
    params.put(AmIpFilter.PROXY_HOST, proxyHost);
    params.put(AmIpFilter.PROXY_URI_BASE, proxyUri);
    FilterConfig config = new DummyFilterConfig(params);

    // dummy filter
    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest,
          ServletResponse servletResponse) throws IOException, ServletException {
        doFilterRequest = servletRequest.getClass().getName();
        if (servletRequest instanceof AmIpServletRequestWrapper) {
          servletWrapper = (AmIpServletRequestWrapper) servletRequest;

        }
      }
    };
    AmIpFilter testFilter = new AmIpFilter();
    testFilter.init(config);

    HttpServletResponseForTest response = new HttpServletResponseForTest();

    // Test request should implements HttpServletRequest
    ServletRequest failRequest = Mockito.mock(ServletRequest.class);
    try {
      testFilter.doFilter(failRequest, response, chain);
      fail();
    } catch (ServletException e) {
      assertEquals(ProxyUtils.E_HTTP_HTTPS_ONLY, e.getMessage());
    }

    // request with HttpServletRequest
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRemoteAddr()).thenReturn("nowhere");
    Mockito.when(request.getRequestURI()).thenReturn("/app/application_00_0");

    // address "redirect" is not in host list for non-proxy connection
    testFilter.doFilter(request, response, chain);
    assertEquals(HttpURLConnection.HTTP_MOVED_TEMP, response.status);
    String redirect = response.getHeader(ProxyUtils.LOCATION);
    assertEquals("http://bogus/app/application_00_0", redirect);

    // address "redirect" is not in host list for proxy connection
    Mockito.when(request.getRequestURI()).thenReturn("/proxy/application_00_0");
    testFilter.doFilter(request, response, chain);
    assertEquals(HttpURLConnection.HTTP_MOVED_TEMP, response.status);
    redirect = response.getHeader(ProxyUtils.LOCATION);
    assertEquals("http://bogus/proxy/redirect/application_00_0", redirect);

    // check for query parameters
    Mockito.when(request.getRequestURI()).thenReturn("/proxy/application_00_0");
    Mockito.when(request.getQueryString()).thenReturn("id=0");
    testFilter.doFilter(request, response, chain);
    assertEquals(HttpURLConnection.HTTP_MOVED_TEMP, response.status);
    redirect = response.getHeader(ProxyUtils.LOCATION);
    assertEquals("http://bogus/proxy/redirect/application_00_0?id=0", redirect);

    // "127.0.0.1" contains in host list. Without cookie
    Mockito.when(request.getRemoteAddr()).thenReturn("127.0.0.1");
    testFilter.doFilter(request, response, chain);
    assertTrue(doFilterRequest.contains("HttpServletRequest"));

    // cookie added
    Cookie[] cookies = new Cookie[] {
        new Cookie(WebAppProxyServlet.PROXY_USER_COOKIE_NAME, "user")
    };

    Mockito.when(request.getCookies()).thenReturn(cookies);
    testFilter.doFilter(request, response, chain);

    assertEquals(
        "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpServletRequestWrapper",
        doFilterRequest);
    // request contains principal from cookie
    assertEquals("user", servletWrapper.getUserPrincipal().getName());
    assertEquals("user", servletWrapper.getRemoteUser());
    assertFalse(servletWrapper.isUserInRole(""));

  }

  private class HttpServletResponseForTest extends HttpServletResponseImpl {
    String redirectLocation = "";
    int status;
    private String contentType;
    private final Map<String, String> headers = new HashMap<>(1);
    private StringWriter body;


    public String getRedirect() {
      return redirectLocation;
    }

    @Override
    public void sendRedirect(String location) throws IOException {
      redirectLocation = location;
    }

    @Override
    public String encodeRedirectURL(String url) {
      return url;
    }

    @Override
    public void setStatus(int status) {
      this.status = status;
    }

    @Override
    public void setContentType(String type) {
      this.contentType = type;
    }

    @Override
    public void setHeader(String name, String value) {
      headers.put(name, value);
    }

    public String getHeader(String name) {
      return headers.get(name);
    }

    @Override
    public PrintWriter getWriter() throws IOException {
      body = new StringWriter();
      return new PrintWriter(body);
    }
  }

  
}

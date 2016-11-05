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
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.*;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.junit.Assert.*;

import org.apache.hadoop.yarn.server.webproxy.ProxyUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
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

    // "127.0.0.1" contains in host list. Without cookie
    Mockito.when(request.getRemoteAddr()).thenReturn("127.0.0.1");
    testFilter.doFilter(request, response, chain);
    assertTrue(doFilterRequest
        .contains("javax.servlet.http.HttpServletRequest"));

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

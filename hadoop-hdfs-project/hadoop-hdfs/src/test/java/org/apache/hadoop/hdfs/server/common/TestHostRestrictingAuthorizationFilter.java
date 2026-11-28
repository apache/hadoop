/*
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
package org.apache.hadoop.hdfs.server.common;

import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.junit.jupiter.api.Test;
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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test Host Restriction Filter.
 */
public class TestHostRestrictingAuthorizationFilter {
  private Logger log =
      LoggerFactory.getLogger(TestHostRestrictingAuthorizationFilter.class);

  /*
   * Test running in unrestricted mode
   */
  @Test
  public void testAcceptAll() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRemoteAddr()).thenReturn(null);
    Mockito.when(request.getMethod()).thenReturn("GET");
    Mockito.when(request.getRequestURI())
        .thenReturn(new StringBuffer(WebHdfsFileSystem.PATH_PREFIX + "/user" +
            "/ubuntu/foo").toString());
    Mockito.when(request.getQueryString()).thenReturn("op=OPEN");
    Mockito.when(request.getRemoteAddr()).thenReturn("192.168.1.2");

    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest,
          ServletResponse servletResponse)
          throws IOException, ServletException {
      }
    };

    Filter filter = new HostRestrictingAuthorizationFilter();

    HashMap<String, String> configs = new HashMap<String, String>() {
    };
    String allowRule = "*,*,/";
    log.trace("Passing configs:\n{}", allowRule);
    configs.put("host.allow.rules", allowRule);
    configs.put(AuthenticationFilter.AUTH_TYPE, "simple");
    FilterConfig fc = new DummyFilterConfig(configs);

    filter.init(fc);
    filter.doFilter(request, response, chain);
    Mockito.verify(response, Mockito.times(0)).sendError(Mockito.eq(HttpServletResponse.SC_FORBIDDEN),
        Mockito.anyString());
    filter.destroy();
  }

  /*
   * Test accepting a GET request for the file checksum when prohibited from
   * doing
   * a GET open call
   */
  @Test
  public void testAcceptGETFILECHECKSUM() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRemoteAddr()).thenReturn(null);
    Mockito.when(request.getMethod()).thenReturn("GET");
    Mockito.when(request.getRequestURI())
        .thenReturn(new StringBuffer(WebHdfsFileSystem.PATH_PREFIX + "/user" +
            "/ubuntu/").toString());
    Mockito.when(request.getQueryString()).thenReturn("op=GETFILECHECKSUM ");
    Mockito.when(request.getRemoteAddr()).thenReturn("192.168.1.2");

    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest,
          ServletResponse servletResponse)
          throws IOException, ServletException {
      }
    };

    Filter filter = new HostRestrictingAuthorizationFilter();

    HashMap<String, String> configs = new HashMap<String, String>() {
    };
    configs.put(AuthenticationFilter.AUTH_TYPE, "simple");
    FilterConfig fc = new DummyFilterConfig(configs);

    filter.init(fc);
    filter.doFilter(request, response, chain);
    Mockito.verify(response, Mockito.times(0)).sendError(Mockito.eq(HttpServletResponse.SC_FORBIDDEN),
        Mockito.anyString());
    filter.destroy();
  }

  /*
   * Test accepting a GET request for reading a file via an open call
   */
  @Test
  public void testRuleAllowedGet() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRemoteAddr()).thenReturn(null);
    Mockito.when(request.getMethod()).thenReturn("GET");
    String queryString = "op=OPEN";
    Mockito.when(request.getRequestURI())
        .thenReturn(new StringBuffer(WebHdfsFileSystem.PATH_PREFIX + "/user" +
            "/ubuntu/foo?" + queryString).toString());
    Mockito.when(request.getQueryString()).thenReturn(queryString);
    Mockito.when(request.getRemoteAddr()).thenReturn("192.168.1.2");

    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest,
          ServletResponse servletResponse)
          throws IOException, ServletException {
      }
    };

    Filter filter = new HostRestrictingAuthorizationFilter();

    HashMap<String, String> configs = new HashMap<String, String>() {
    };
    String allowRule = "ubuntu,127.0.0.1/32,/localbits/*|*,192.168.0.1/22," +
        "/user/ubuntu/*";
    log.trace("Passing configs:\n{}", allowRule);
    configs.put("host.allow.rules", allowRule);
    configs.put(AuthenticationFilter.AUTH_TYPE, "simple");
    FilterConfig fc = new DummyFilterConfig(configs);

    filter.init(fc);
    filter.doFilter(request, response, chain);
    filter.destroy();
  }

  /*
   * Test by default we deny an open call GET request
   */
  @Test
  public void testRejectsGETs() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRemoteAddr()).thenReturn(null);
    Mockito.when(request.getMethod()).thenReturn("GET");
    String queryString = "bar=foo&delegationToken=dt&op=OPEN";
    Mockito.when(request.getRequestURI())
        .thenReturn(new StringBuffer(WebHdfsFileSystem.PATH_PREFIX + "/user" +
            "/ubuntu/?" + queryString).toString());
    Mockito.when(request.getQueryString()).thenReturn(queryString);
    Mockito.when(request.getRemoteAddr()).thenReturn("192.168.1.2");

    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest,
          ServletResponse servletResponse)
          throws IOException, ServletException {
      }
    };

    Filter filter = new HostRestrictingAuthorizationFilter();

    HashMap<String, String> configs = new HashMap<String, String>() {
    };
    configs.put(AuthenticationFilter.AUTH_TYPE, "simple");
    FilterConfig fc = new DummyFilterConfig(configs);

    filter.init(fc);
    filter.doFilter(request, response, chain);
    filter.destroy();
  }

  /*
   * Test acceptable behavior to malformed requests
   * Case: no operation (op parameter) specified
   */
  @Test
  public void testUnexpectedInputMissingOpParameter() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRemoteAddr()).thenReturn(null);
    Mockito.when(request.getMethod()).thenReturn("GET");
    Mockito.when(request.getRequestURI())
        .thenReturn(new StringBuffer(WebHdfsFileSystem.PATH_PREFIX +
            "/IAmARandomRequest/").toString());
    Mockito.when(request.getQueryString()).thenReturn(null);
    Mockito.when(request.getRemoteAddr()).thenReturn("192.168.1.2");

    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest,
          ServletResponse servletResponse)
          throws IOException, ServletException {
      }
    };

    Filter filter = new HostRestrictingAuthorizationFilter();

    HashMap<String, String> configs = new HashMap<String, String>() {
    };
    configs.put(AuthenticationFilter.AUTH_TYPE, "simple");
    FilterConfig fc = new DummyFilterConfig(configs);

    filter.init(fc);
    filter.doFilter(request, response, chain);
    log.error("XXX {}", response.getStatus());
    filter.destroy();
  }

  /**
   * A request that don't access WebHDFS API should pass through.
   */
  @Test
  public void testNotWebhdfsAPIRequest() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getMethod()).thenReturn("GET");
    Mockito.when(request.getRequestURI()).thenReturn("/conf");
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

    Filter filter = new HostRestrictingAuthorizationFilter();
    HashMap<String, String> configs = new HashMap<String, String>() {};
    configs.put(AuthenticationFilter.AUTH_TYPE, "simple");
    FilterConfig fc = new DummyFilterConfig(configs);

    filter.init(fc);
    filter.doFilter(request, response, (servletRequest, servletResponse) -> {});
    filter.destroy();
  }

  /*
   * Test IPv4 and IPv6 clients against IPv4 and IPv6 rules. A rule of the
   * other address family does not match.
   */
  @Test
  public void testIPv4AndIPv6Rules() throws Exception {
    Filter filter = new HostRestrictingAuthorizationFilter();
    filter.init(newFilterConfig("*,192.168.0.0/16,/v4|*,2001:db8::/32,/v6"));

    assertOpenAllowed(filter, "192.168.1.2", "/v4/foo", true);
    assertOpenAllowed(filter, "10.0.0.1", "/v4/foo", false);
    assertOpenAllowed(filter, "192.168.1.2", "/v6/foo", false);
    assertOpenAllowed(filter, "2001:db8:0:0:0:0:0:1", "/v6/foo", true);
    // Jetty returns an IPv6 client address in brackets
    assertOpenAllowed(filter, "[2001:db8::1]", "/v6/foo", true);
    assertOpenAllowed(filter, "2001:db9:0:0:0:0:0:1", "/v6/foo", false);
    assertOpenAllowed(filter, "2001:db8:0:0:0:0:0:1", "/v4/foo", false);
    filter.destroy();
  }

  /*
   * Test that a rule network other than "*" or network/bits fails the filter
   * initialization.
   */
  @Test
  public void testRejectsInvalidNetwork() {
    for (String network : new String[] {"2001:db8::/129", "2001:db8::1", "192.168.0.1"}) {
      Filter filter = new HostRestrictingAuthorizationFilter();
      assertThatThrownBy(() -> filter.init(newFilterConfig("*," + network + ",/")))
          .as(network)
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  private static FilterConfig newFilterConfig(String allowRules) {
    Map<String, String> configs = new HashMap<>();
    configs.put("host.allow.rules", allowRules);
    configs.put(AuthenticationFilter.AUTH_TYPE, "simple");
    return new DummyFilterConfig(configs);
  }

  /*
   * Sends an op=OPEN request and checks that it either proceeds or gets 403.
   */
  private static void assertOpenAllowed(Filter filter, String remoteAddr,
      String path, boolean allowed) throws IOException, ServletException {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getMethod()).thenReturn("GET");
    Mockito.when(request.getRequestURI())
        .thenReturn(WebHdfsFileSystem.PATH_PREFIX + path);
    Mockito.when(request.getQueryString()).thenReturn("op=OPEN");
    Mockito.when(request.getRemoteAddr()).thenReturn(remoteAddr);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    FilterChain chain = Mockito.mock(FilterChain.class);

    filter.doFilter(request, response, chain);
    Mockito.verify(chain, Mockito.times(allowed ? 1 : 0))
        .doFilter(request, response);
    Mockito.verify(response, Mockito.times(allowed ? 0 : 1))
        .sendError(Mockito.eq(HttpServletResponse.SC_FORBIDDEN), Mockito.anyString());
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
      ServletContext context = Mockito.mock(ServletContext.class);
      Mockito.when(context.getAttribute(AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE)).thenReturn(null);
      return context;
    }
  }
}

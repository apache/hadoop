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

package org.apache.hadoop.yarn.server.timeline.webapp;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TestCrossOriginFilter {

  @Test
  public void testSameOrigin() throws ServletException, IOException {

    // Setup the configuration settings of the server
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "");
    FilterConfig filterConfig = new FilterConfigTest(conf);

    // Origin is not specified for same origin requests
    HttpServletRequest mockReq = mock(HttpServletRequest.class);
    when(mockReq.getHeader(CrossOriginFilter.ORIGIN)).thenReturn(null);

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = mock(HttpServletResponse.class);
    FilterChain mockChain = mock(FilterChain.class);

    // Object under test
    CrossOriginFilter filter = new CrossOriginFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    verifyZeroInteractions(mockRes);
    verify(mockChain).doFilter(mockReq, mockRes);
  }

  @Test
  public void testDisallowedOrigin() throws ServletException, IOException {

    // Setup the configuration settings of the server
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "example.com");
    FilterConfig filterConfig = new FilterConfigTest(conf);

    // Origin is not specified for same origin requests
    HttpServletRequest mockReq = mock(HttpServletRequest.class);
    when(mockReq.getHeader(CrossOriginFilter.ORIGIN)).thenReturn("example.org");

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = mock(HttpServletResponse.class);
    FilterChain mockChain = mock(FilterChain.class);

    // Object under test
    CrossOriginFilter filter = new CrossOriginFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    verifyZeroInteractions(mockRes);
    verify(mockChain).doFilter(mockReq, mockRes);
  }

  @Test
  public void testDisallowedMethod() throws ServletException, IOException {

    // Setup the configuration settings of the server
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "example.com");
    FilterConfig filterConfig = new FilterConfigTest(conf);

    // Origin is not specified for same origin requests
    HttpServletRequest mockReq = mock(HttpServletRequest.class);
    when(mockReq.getHeader(CrossOriginFilter.ORIGIN)).thenReturn("example.com");
    when(mockReq.getHeader(CrossOriginFilter.ACCESS_CONTROL_REQUEST_METHOD))
        .thenReturn("DISALLOWED_METHOD");

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = mock(HttpServletResponse.class);
    FilterChain mockChain = mock(FilterChain.class);

    // Object under test
    CrossOriginFilter filter = new CrossOriginFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    verifyZeroInteractions(mockRes);
    verify(mockChain).doFilter(mockReq, mockRes);
  }

  @Test
  public void testDisallowedHeader() throws ServletException, IOException {

    // Setup the configuration settings of the server
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "example.com");
    FilterConfig filterConfig = new FilterConfigTest(conf);

    // Origin is not specified for same origin requests
    HttpServletRequest mockReq = mock(HttpServletRequest.class);
    when(mockReq.getHeader(CrossOriginFilter.ORIGIN)).thenReturn("example.com");
    when(mockReq.getHeader(CrossOriginFilter.ACCESS_CONTROL_REQUEST_METHOD))
        .thenReturn("GET");
    when(mockReq.getHeader(CrossOriginFilter.ACCESS_CONTROL_REQUEST_HEADERS))
        .thenReturn("Disallowed-Header");

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = mock(HttpServletResponse.class);
    FilterChain mockChain = mock(FilterChain.class);

    // Object under test
    CrossOriginFilter filter = new CrossOriginFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    verifyZeroInteractions(mockRes);
    verify(mockChain).doFilter(mockReq, mockRes);
  }

  @Test
  public void testCrossOriginFilter() throws ServletException, IOException {

    // Setup the configuration settings of the server
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "example.com");
    FilterConfig filterConfig = new FilterConfigTest(conf);

    // Origin is not specified for same origin requests
    HttpServletRequest mockReq = mock(HttpServletRequest.class);
    when(mockReq.getHeader(CrossOriginFilter.ORIGIN)).thenReturn("example.com");
    when(mockReq.getHeader(CrossOriginFilter.ACCESS_CONTROL_REQUEST_METHOD))
        .thenReturn("GET");
    when(mockReq.getHeader(CrossOriginFilter.ACCESS_CONTROL_REQUEST_HEADERS))
        .thenReturn("X-Requested-With");

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = mock(HttpServletResponse.class);
    FilterChain mockChain = mock(FilterChain.class);

    // Object under test
    CrossOriginFilter filter = new CrossOriginFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    verify(mockRes).setHeader(CrossOriginFilter.ACCESS_CONTROL_ALLOW_ORIGIN,
        "example.com");
    verify(mockRes).setHeader(
        CrossOriginFilter.ACCESS_CONTROL_ALLOW_CREDENTIALS,
        Boolean.TRUE.toString());
    verify(mockRes).setHeader(CrossOriginFilter.ACCESS_CONTROL_ALLOW_METHODS,
        filter.getAllowedMethodsHeader());
    verify(mockRes).setHeader(CrossOriginFilter.ACCESS_CONTROL_ALLOW_HEADERS,
        filter.getAllowedHeadersHeader());
    verify(mockChain).doFilter(mockReq, mockRes);
  }

  private static class FilterConfigTest implements FilterConfig {

    final Map<String, String> map;

    FilterConfigTest(Map<String, String> map) {
      this.map = map;
    }

    @Override
    public String getFilterName() {
      return "test-filter";
    }

    @Override
    public String getInitParameter(String key) {
      return map.get(key);
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
}

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

package org.apache.hadoop.security.http;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;
import org.mockito.Mockito;

public class TestRestCsrfPreventionFilter {

  private static final String EXPECTED_MESSAGE =
      "Missing Required Header for Vulnerability Protection";
  private static final String X_CUSTOM_HEADER = "X-CUSTOM_HEADER";

  @Test
  public void testNoHeaderDefaultConfig_badRequest()
      throws ServletException, IOException {
    // Setup the configuration settings of the server
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_HEADER_PARAM)).thenReturn(null);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_METHODS_TO_IGNORE_PARAM)).
      thenReturn(null);

    // CSRF has not been sent
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(RestCsrfPreventionFilter.HEADER_DEFAULT)).
      thenReturn(null);

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
    FilterChain mockChain = Mockito.mock(FilterChain.class);

    // Object under test
    RestCsrfPreventionFilter filter = new RestCsrfPreventionFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    verify(mockRes, atLeastOnce()).sendError(
        HttpServletResponse.SC_BAD_REQUEST, EXPECTED_MESSAGE);
    Mockito.verifyZeroInteractions(mockChain);
  }

  @Test
  public void testHeaderPresentDefaultConfig_goodRequest()
      throws ServletException, IOException {
    // Setup the configuration settings of the server
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_HEADER_PARAM)).thenReturn(null);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_METHODS_TO_IGNORE_PARAM)).
      thenReturn(null);

    // CSRF HAS been sent
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(RestCsrfPreventionFilter.HEADER_DEFAULT)).
      thenReturn("valueUnimportant");

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
    FilterChain mockChain = Mockito.mock(FilterChain.class);

    // Object under test
    RestCsrfPreventionFilter filter = new RestCsrfPreventionFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    Mockito.verify(mockChain).doFilter(mockReq, mockRes);
  }

  @Test
  public void testHeaderPresentCustomHeaderConfig_goodRequest()
      throws ServletException, IOException {
    // Setup the configuration settings of the server
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_HEADER_PARAM)).
      thenReturn(X_CUSTOM_HEADER);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_METHODS_TO_IGNORE_PARAM)).
      thenReturn(null);

    // CSRF HAS been sent
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(X_CUSTOM_HEADER)).
      thenReturn("valueUnimportant");

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
    FilterChain mockChain = Mockito.mock(FilterChain.class);

    // Object under test
    RestCsrfPreventionFilter filter = new RestCsrfPreventionFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    Mockito.verify(mockChain).doFilter(mockReq, mockRes);
  }

  @Test
  public void testMissingHeaderWithCustomHeaderConfig_badRequest()
      throws ServletException, IOException {
    // Setup the configuration settings of the server
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_HEADER_PARAM)).
      thenReturn(X_CUSTOM_HEADER);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_METHODS_TO_IGNORE_PARAM)).
      thenReturn(null);

    // CSRF has not been sent
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(RestCsrfPreventionFilter.HEADER_DEFAULT)).
      thenReturn(null);

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
    FilterChain mockChain = Mockito.mock(FilterChain.class);

    // Object under test
    RestCsrfPreventionFilter filter = new RestCsrfPreventionFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    Mockito.verifyZeroInteractions(mockChain);
  }

  @Test
  public void testMissingHeaderNoMethodsToIgnoreConfig_badRequest()
      throws ServletException, IOException {
    // Setup the configuration settings of the server
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_HEADER_PARAM)).thenReturn(null);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_METHODS_TO_IGNORE_PARAM)).
      thenReturn("");

    // CSRF has not been sent
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(RestCsrfPreventionFilter.HEADER_DEFAULT)).
      thenReturn(null);
    Mockito.when(mockReq.getMethod()).
      thenReturn("GET");

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
    FilterChain mockChain = Mockito.mock(FilterChain.class);

    // Object under test
    RestCsrfPreventionFilter filter = new RestCsrfPreventionFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    Mockito.verifyZeroInteractions(mockChain);
  }

  @Test
  public void testMissingHeaderIgnoreGETMethodConfig_goodRequest()
      throws ServletException, IOException {
    // Setup the configuration settings of the server
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_HEADER_PARAM)).thenReturn(null);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_METHODS_TO_IGNORE_PARAM)).
      thenReturn("GET");

    // CSRF has not been sent
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(RestCsrfPreventionFilter.HEADER_DEFAULT)).
      thenReturn(null);
    Mockito.when(mockReq.getMethod()).
      thenReturn("GET");

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
    FilterChain mockChain = Mockito.mock(FilterChain.class);

    // Object under test
    RestCsrfPreventionFilter filter = new RestCsrfPreventionFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    Mockito.verify(mockChain).doFilter(mockReq, mockRes);
  }

  @Test
  public void testMissingHeaderMultipleIgnoreMethodsConfig_goodRequest()
      throws ServletException, IOException {
    // Setup the configuration settings of the server
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_HEADER_PARAM)).thenReturn(null);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_METHODS_TO_IGNORE_PARAM)).
      thenReturn("GET,OPTIONS");

    // CSRF has not been sent
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(RestCsrfPreventionFilter.HEADER_DEFAULT)).
      thenReturn(null);
    Mockito.when(mockReq.getMethod()).
      thenReturn("OPTIONS");

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
    FilterChain mockChain = Mockito.mock(FilterChain.class);

    // Object under test
    RestCsrfPreventionFilter filter = new RestCsrfPreventionFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    Mockito.verify(mockChain).doFilter(mockReq, mockRes);
  }

  @Test
  public void testMissingHeaderMultipleIgnoreMethodsConfig_badRequest()
      throws ServletException, IOException {
    // Setup the configuration settings of the server
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_HEADER_PARAM)).thenReturn(null);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_METHODS_TO_IGNORE_PARAM)).
      thenReturn("GET,OPTIONS");

    // CSRF has not been sent
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(RestCsrfPreventionFilter.HEADER_DEFAULT)).
      thenReturn(null);
    Mockito.when(mockReq.getMethod()).
      thenReturn("PUT");

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
    FilterChain mockChain = Mockito.mock(FilterChain.class);

    // Object under test
    RestCsrfPreventionFilter filter = new RestCsrfPreventionFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    Mockito.verifyZeroInteractions(mockChain);
  }
}

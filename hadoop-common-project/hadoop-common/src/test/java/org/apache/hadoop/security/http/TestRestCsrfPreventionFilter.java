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

/**
 * This class tests the behavior of the RestCsrfPreventionFilter.
 *
 */
public class TestRestCsrfPreventionFilter {

  private static final String NON_BROWSER = "java";
  private static final String BROWSER_AGENT =
      "Mozilla/5.0 (compatible; U; ABrowse 0.6; Syllable)" +
      " AppleWebKit/420+ (KHTML, like Gecko)";
  private static final String EXPECTED_MESSAGE =
      "Missing Required Header for CSRF Vulnerability Protection";
  private static final String X_CUSTOM_HEADER = "X-CUSTOM_HEADER";

  @Test
  public void testNoHeaderDefaultConfigBadRequest()
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
    Mockito.when(mockReq.getHeader(RestCsrfPreventionFilter.HEADER_USER_AGENT)).
      thenReturn(BROWSER_AGENT);

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
  public void testNoHeaderCustomAgentConfigBadRequest()
      throws ServletException, IOException {
    // Setup the configuration settings of the server
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_HEADER_PARAM)).thenReturn(null);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_METHODS_TO_IGNORE_PARAM)).
      thenReturn(null);
    Mockito.when(filterConfig.getInitParameter(
        RestCsrfPreventionFilter.BROWSER_USER_AGENT_PARAM)).
        thenReturn("^Mozilla.*,^Opera.*,curl");

    // CSRF has not been sent
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(RestCsrfPreventionFilter.HEADER_DEFAULT)).
      thenReturn(null);
    Mockito.when(mockReq.getHeader(RestCsrfPreventionFilter.HEADER_USER_AGENT)).
      thenReturn("curl");

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
  public void testNoHeaderDefaultConfigNonBrowserGoodRequest()
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
    Mockito.when(mockReq.getHeader(RestCsrfPreventionFilter.HEADER_USER_AGENT)).
      thenReturn(NON_BROWSER);

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
  public void testHeaderPresentDefaultConfigGoodRequest()
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
  public void testHeaderPresentCustomHeaderConfigGoodRequest()
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
  public void testMissingHeaderWithCustomHeaderConfigBadRequest()
      throws ServletException, IOException {
    // Setup the configuration settings of the server
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_HEADER_PARAM)).
      thenReturn(X_CUSTOM_HEADER);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_METHODS_TO_IGNORE_PARAM)).
      thenReturn(null);
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(RestCsrfPreventionFilter.HEADER_USER_AGENT)).
      thenReturn(BROWSER_AGENT);

    // CSRF has not been sent
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
  public void testMissingHeaderNoMethodsToIgnoreConfigBadRequest()
      throws ServletException, IOException {
    // Setup the configuration settings of the server
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_HEADER_PARAM)).thenReturn(null);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_METHODS_TO_IGNORE_PARAM)).
      thenReturn("");
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(RestCsrfPreventionFilter.HEADER_USER_AGENT)).
      thenReturn(BROWSER_AGENT);

    // CSRF has not been sent
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
  public void testMissingHeaderIgnoreGETMethodConfigGoodRequest()
      throws ServletException, IOException {
    // Setup the configuration settings of the server
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_HEADER_PARAM)).thenReturn(null);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_METHODS_TO_IGNORE_PARAM)).
      thenReturn("GET");
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(RestCsrfPreventionFilter.HEADER_USER_AGENT)).
      thenReturn(BROWSER_AGENT);

    // CSRF has not been sent
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
  public void testMissingHeaderMultipleIgnoreMethodsConfigGoodRequest()
      throws ServletException, IOException {
    // Setup the configuration settings of the server
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_HEADER_PARAM)).thenReturn(null);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_METHODS_TO_IGNORE_PARAM)).
      thenReturn("GET,OPTIONS");
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(RestCsrfPreventionFilter.HEADER_USER_AGENT)).
      thenReturn(BROWSER_AGENT);

    // CSRF has not been sent
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
  public void testMissingHeaderMultipleIgnoreMethodsConfigBadRequest()
      throws ServletException, IOException {
    // Setup the configuration settings of the server
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_HEADER_PARAM)).thenReturn(null);
    Mockito.when(filterConfig.getInitParameter(
      RestCsrfPreventionFilter.CUSTOM_METHODS_TO_IGNORE_PARAM)).
      thenReturn("GET,OPTIONS");
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(RestCsrfPreventionFilter.HEADER_USER_AGENT)).
      thenReturn(BROWSER_AGENT);

    // CSRF has not been sent
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

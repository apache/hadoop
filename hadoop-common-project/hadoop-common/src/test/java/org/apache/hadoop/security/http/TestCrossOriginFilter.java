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

import org.apache.hadoop.security.http.CrossOriginFilter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestCrossOriginFilter {

  @Test
  public void testSameOrigin() throws ServletException, IOException {

    // Setup the configuration settings of the server
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "");
    FilterConfig filterConfig = new FilterConfigTest(conf);

    // Origin is not specified for same origin requests
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(CrossOriginFilter.ORIGIN)).thenReturn(null);

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
    FilterChain mockChain = Mockito.mock(FilterChain.class);

    // Object under test
    CrossOriginFilter filter = new CrossOriginFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    Mockito.verifyZeroInteractions(mockRes);
    Mockito.verify(mockChain).doFilter(mockReq, mockRes);
  }

  @Test
  public void testAllowAllOrigins() throws ServletException, IOException {

    // Setup the configuration settings of the server
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "*");
    FilterConfig filterConfig = new FilterConfigTest(conf);

    // Object under test
    CrossOriginFilter filter = new CrossOriginFilter();
    filter.init(filterConfig);
    Assert.assertTrue(filter.areOriginsAllowed("example.com"));
  }

  @Test
  public void testEncodeHeaders() {
    String validOrigin = "http://localhost:12345";
    String encodedValidOrigin = CrossOriginFilter.encodeHeader(validOrigin);
    Assert.assertEquals("Valid origin encoding should match exactly",
        validOrigin, encodedValidOrigin);

    String httpResponseSplitOrigin = validOrigin + " \nSecondHeader: value";
    String encodedResponseSplitOrigin =
      CrossOriginFilter.encodeHeader(httpResponseSplitOrigin);
    Assert.assertEquals("Http response split origin should be protected against",
        validOrigin, encodedResponseSplitOrigin);

    // Test Origin List
    String validOriginList = "http://foo.example.com:12345 http://bar.example.com:12345";
    String encodedValidOriginList = CrossOriginFilter
        .encodeHeader(validOriginList);
    Assert.assertEquals("Valid origin list encoding should match exactly",
        validOriginList, encodedValidOriginList);
  }

  @Test
  public void testPatternMatchingOrigins() throws ServletException, IOException {

    // Setup the configuration settings of the server
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "*.example.com");
    FilterConfig filterConfig = new FilterConfigTest(conf);

    // Object under test
    CrossOriginFilter filter = new CrossOriginFilter();
    filter.init(filterConfig);

    // match multiple sub-domains
    Assert.assertFalse(filter.areOriginsAllowed("example.com"));
    Assert.assertFalse(filter.areOriginsAllowed("foo:example.com"));
    Assert.assertTrue(filter.areOriginsAllowed("foo.example.com"));
    Assert.assertTrue(filter.areOriginsAllowed("foo.bar.example.com"));

    // First origin is allowed
    Assert.assertTrue(filter.areOriginsAllowed("foo.example.com foo.nomatch.com"));
    // Second origin is allowed
    Assert.assertTrue(filter.areOriginsAllowed("foo.nomatch.com foo.example.com"));
    // No origin in list is allowed
    Assert.assertFalse(filter.areOriginsAllowed("foo.nomatch1.com foo.nomatch2.com"));
  }

  @Test
  public void testRegexPatternMatchingOrigins() throws ServletException, IOException {

    // Setup the configuration settings of the server
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "regex:.*[.]example[.]com");
    FilterConfig filterConfig = new FilterConfigTest(conf);

    // Object under test
    CrossOriginFilter filter = new CrossOriginFilter();
    filter.init(filterConfig);

    // match multiple sub-domains
    Assert.assertFalse(filter.areOriginsAllowed("example.com"));
    Assert.assertFalse(filter.areOriginsAllowed("foo:example.com"));
    Assert.assertTrue(filter.areOriginsAllowed("foo.example.com"));
    Assert.assertTrue(filter.areOriginsAllowed("foo.bar.example.com"));

    // First origin is allowed
    Assert.assertTrue(filter.areOriginsAllowed("foo.example.com foo.nomatch.com"));
    // Second origin is allowed
    Assert.assertTrue(filter.areOriginsAllowed("foo.nomatch.com foo.example.com"));
    // No origin in list is allowed
    Assert.assertFalse(filter.areOriginsAllowed("foo.nomatch1.com foo.nomatch2.com"));
  }

  @Test
  public void testComplexRegexPatternMatchingOrigins() throws ServletException, IOException {

    // Setup the configuration settings of the server
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "regex:https?:\\/\\/sub1[.]example[.]com(:[0-9]+)?");
    FilterConfig filterConfig = new FilterConfigTest(conf);

    // Object under test
    CrossOriginFilter filter = new CrossOriginFilter();
    filter.init(filterConfig);

    Assert.assertTrue(filter.areOriginsAllowed("http://sub1.example.com"));
    Assert.assertTrue(filter.areOriginsAllowed("https://sub1.example.com"));
    Assert.assertTrue(filter.areOriginsAllowed("http://sub1.example.com:1234"));
    Assert.assertTrue(filter.areOriginsAllowed("https://sub1.example.com:8080"));

    // No origin in list is allowed
    Assert.assertFalse(filter.areOriginsAllowed("foo.nomatch1.com foo.nomatch2.com"));
  }

  @Test
  public void testMixedRegexPatternMatchingOrigins() throws ServletException, IOException {

    // Setup the configuration settings of the server
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "regex:https?:\\/\\/sub1[.]example[.]com(:[0-9]+)?, "
            + "*.example2.com");
    FilterConfig filterConfig = new FilterConfigTest(conf);

    // Object under test
    CrossOriginFilter filter = new CrossOriginFilter();
    filter.init(filterConfig);

    Assert.assertTrue(filter.areOriginsAllowed("http://sub1.example.com"));
    Assert.assertTrue(filter.areOriginsAllowed("https://sub1.example.com"));
    Assert.assertTrue(filter.areOriginsAllowed("http://sub1.example.com:1234"));
    Assert.assertTrue(filter.areOriginsAllowed("https://sub1.example.com:8080"));

    // match multiple sub-domains
    Assert.assertFalse(filter.areOriginsAllowed("example2.com"));
    Assert.assertFalse(filter.areOriginsAllowed("foo:example2.com"));
    Assert.assertTrue(filter.areOriginsAllowed("foo.example2.com"));
    Assert.assertTrue(filter.areOriginsAllowed("foo.bar.example2.com"));

    // First origin is allowed
    Assert.assertTrue(filter.areOriginsAllowed("foo.example2.com foo.nomatch.com"));
    // Second origin is allowed
    Assert.assertTrue(filter.areOriginsAllowed("foo.nomatch.com foo.example2.com"));
    // No origin in list is allowed
    Assert.assertFalse(filter.areOriginsAllowed("foo.nomatch1.com foo.nomatch2.com"));
  }

  @Test
  public void testDisallowedOrigin() throws ServletException, IOException {

    // Setup the configuration settings of the server
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "example.com");
    FilterConfig filterConfig = new FilterConfigTest(conf);

    // Origin is not specified for same origin requests
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(CrossOriginFilter.ORIGIN)).thenReturn("example.org");

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
    FilterChain mockChain = Mockito.mock(FilterChain.class);

    // Object under test
    CrossOriginFilter filter = new CrossOriginFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    Mockito.verifyZeroInteractions(mockRes);
    Mockito.verify(mockChain).doFilter(mockReq, mockRes);
  }

  @Test
  public void testDisallowedMethod() throws ServletException, IOException {

    // Setup the configuration settings of the server
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "example.com");
    FilterConfig filterConfig = new FilterConfigTest(conf);

    // Origin is not specified for same origin requests
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(CrossOriginFilter.ORIGIN)).thenReturn("example.com");
    Mockito.when(
        mockReq.getHeader(CrossOriginFilter.ACCESS_CONTROL_REQUEST_METHOD))
        .thenReturn("DISALLOWED_METHOD");

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
    FilterChain mockChain = Mockito.mock(FilterChain.class);

    // Object under test
    CrossOriginFilter filter = new CrossOriginFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    Mockito.verifyZeroInteractions(mockRes);
    Mockito.verify(mockChain).doFilter(mockReq, mockRes);
  }

  @Test
  public void testDisallowedHeader() throws ServletException, IOException {

    // Setup the configuration settings of the server
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "example.com");
    FilterConfig filterConfig = new FilterConfigTest(conf);

    // Origin is not specified for same origin requests
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(CrossOriginFilter.ORIGIN)).thenReturn("example.com");
    Mockito.when(
        mockReq.getHeader(CrossOriginFilter.ACCESS_CONTROL_REQUEST_METHOD))
        .thenReturn("GET");
    Mockito.when(
        mockReq.getHeader(CrossOriginFilter.ACCESS_CONTROL_REQUEST_HEADERS))
        .thenReturn("Disallowed-Header");

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
    FilterChain mockChain = Mockito.mock(FilterChain.class);

    // Object under test
    CrossOriginFilter filter = new CrossOriginFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    Mockito.verifyZeroInteractions(mockRes);
    Mockito.verify(mockChain).doFilter(mockReq, mockRes);
  }

  @Test
  public void testCrossOriginFilter() throws ServletException, IOException {

    // Setup the configuration settings of the server
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "example.com");
    FilterConfig filterConfig = new FilterConfigTest(conf);

    // Origin is not specified for same origin requests
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader(CrossOriginFilter.ORIGIN)).thenReturn("example.com");
    Mockito.when(
        mockReq.getHeader(CrossOriginFilter.ACCESS_CONTROL_REQUEST_METHOD))
        .thenReturn("GET");
    Mockito.when(
        mockReq.getHeader(CrossOriginFilter.ACCESS_CONTROL_REQUEST_HEADERS))
        .thenReturn("X-Requested-With");

    // Objects to verify interactions based on request
    HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
    FilterChain mockChain = Mockito.mock(FilterChain.class);

    // Object under test
    CrossOriginFilter filter = new CrossOriginFilter();
    filter.init(filterConfig);
    filter.doFilter(mockReq, mockRes, mockChain);

    Mockito.verify(mockRes).setHeader(CrossOriginFilter.ACCESS_CONTROL_ALLOW_ORIGIN,
        "example.com");
    Mockito.verify(mockRes).setHeader(
        CrossOriginFilter.ACCESS_CONTROL_ALLOW_CREDENTIALS,
        Boolean.TRUE.toString());
    Mockito.verify(mockRes).setHeader(CrossOriginFilter.ACCESS_CONTROL_ALLOW_METHODS,
        filter.getAllowedMethodsHeader());
    Mockito.verify(mockRes).setHeader(CrossOriginFilter.ACCESS_CONTROL_ALLOW_HEADERS,
        filter.getAllowedHeadersHeader());
    Mockito.verify(mockChain).doFilter(mockReq, mockRes);
  }

  @Test
  public void testCrossOriginFilterAfterRestart() throws ServletException {

    // Setup the configuration settings of the server
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "example.com");
    conf.put(CrossOriginFilter.ALLOWED_HEADERS, "X-Requested-With,Accept");
    conf.put(CrossOriginFilter.ALLOWED_METHODS, "GET,POST");
    FilterConfig filterConfig = new FilterConfigTest(conf);

    // Object under test
    CrossOriginFilter filter = new CrossOriginFilter();
    filter.init(filterConfig);

    //verify filter values
    Assert.assertTrue("Allowed headers do not match",
        filter.getAllowedHeadersHeader()
        .compareTo("X-Requested-With,Accept") == 0);
    Assert.assertTrue("Allowed methods do not match",
        filter.getAllowedMethodsHeader()
        .compareTo("GET,POST") == 0);
    Assert.assertTrue(filter.areOriginsAllowed("example.com"));

    //destroy filter values and clear conf
    filter.destroy();
    conf.clear();

    // Setup the configuration settings of the server
    conf.put(CrossOriginFilter.ALLOWED_ORIGINS, "newexample.com");
    conf.put(CrossOriginFilter.ALLOWED_HEADERS, "Content-Type,Origin");
    conf.put(CrossOriginFilter.ALLOWED_METHODS, "GET,HEAD");
    filterConfig = new FilterConfigTest(conf);

    //initialize filter
    filter.init(filterConfig);

    //verify filter values
    Assert.assertTrue("Allowed headers do not match",
        filter.getAllowedHeadersHeader()
        .compareTo("Content-Type,Origin") == 0);
    Assert.assertTrue("Allowed methods do not match",
        filter.getAllowedMethodsHeader()
        .compareTo("GET,HEAD") == 0);
    Assert.assertTrue(filter.areOriginsAllowed("newexample.com"));

    //destroy filter values
    filter.destroy();
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

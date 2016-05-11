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

import java.util.Collection;
import java.util.ArrayList;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Assert;
import org.junit.Test;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test the default and customized behaviors of XFrameOptionsFilter.
 *
 */
public class TestXFrameOptionsFilter {
  private static final String X_FRAME_OPTIONS = "X-Frame-Options";

  @Test
  public void testDefaultOptionsValue() throws Exception {
    final Collection<String> headers = new ArrayList<String>();
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getInitParameter(
        XFrameOptionsFilter.CUSTOM_HEADER_PARAM)).thenReturn(null);

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    FilterChain chain = Mockito.mock(FilterChain.class);

    Mockito.doAnswer(
        new Answer() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          Object[] args = invocation.getArguments();
          Assert.assertTrue(
              "header should be visible inside chain and filters.",
              ((HttpServletResponse)args[1]).
              containsHeader(X_FRAME_OPTIONS));
            return null;
          }
        }
       ).when(chain).doFilter(Mockito.<ServletRequest>anyObject(),
          Mockito.<ServletResponse>anyObject());

    Mockito.doAnswer(
        new Answer() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
            Object[] args = invocation.getArguments();
            Assert.assertTrue(
                "Options value incorrect should be DENY but is: "
                + args[1], "DENY".equals(args[1]));
            headers.add((String)args[1]);
            return null;
          }
        }
       ).when(response).setHeader(Mockito.<String>anyObject(),
        Mockito.<String>anyObject());

    XFrameOptionsFilter filter = new XFrameOptionsFilter();
    filter.init(filterConfig);

    filter.doFilter(request, response, chain);

    Assert.assertEquals("X-Frame-Options count not equal to 1.",
        headers.size(), 1);
  }

  @Test
  public void testCustomOptionsValueAndNoOverrides() throws Exception {
    final Collection<String> headers = new ArrayList<String>();
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getInitParameter(
        XFrameOptionsFilter.CUSTOM_HEADER_PARAM)).thenReturn("SAMEORIGIN");

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    final HttpServletResponse response =
        Mockito.mock(HttpServletResponse.class);
    FilterChain chain = Mockito.mock(FilterChain.class);

    Mockito.doAnswer(
        new Answer() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          Object[] args = invocation.getArguments();
          HttpServletResponse resp = (HttpServletResponse) args[1];
          Assert.assertTrue(
              "Header should be visible inside chain and filters.",
              resp.containsHeader(X_FRAME_OPTIONS));
          // let's try and set another value for the header and make
          // sure that it doesn't overwrite the configured value
          Assert.assertTrue(resp instanceof
              XFrameOptionsFilter.XFrameOptionsResponseWrapper);
          resp.setHeader(X_FRAME_OPTIONS, "LJM");
          return null;
          }
        }
       ).when(chain).doFilter(Mockito.<ServletRequest>anyObject(),
          Mockito.<ServletResponse>anyObject());

    Mockito.doAnswer(
        new Answer() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
            Object[] args = invocation.getArguments();
            Assert.assertEquals(
                "Options value incorrect should be SAMEORIGIN but is: "
                + args[1], "SAMEORIGIN", args[1]);
            headers.add((String)args[1]);
            return null;
          }
        }
       ).when(response).setHeader(Mockito.<String>anyObject(),
        Mockito.<String>anyObject());

    XFrameOptionsFilter filter = new XFrameOptionsFilter();
    filter.init(filterConfig);

    filter.doFilter(request, response, chain);

    Assert.assertEquals("X-Frame-Options count not equal to 1.",
        headers.size(), 1);

    Assert.assertEquals("X-Frame-Options count not equal to 1.",
        headers.toArray()[0], "SAMEORIGIN");
  }
}

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
package org.apache.hadoop.crypto.key.kms.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test for {@link KMSMDCFilter}.
 *
 */
public class TestKMSMDCFilter {

  private static final String REMOTE_ADDRESS = "192.168.100.100";
  private static final String URL = "/admin";
  private static final String METHOD = "GET";

  private KMSMDCFilter filter;
  private HttpServletRequest httpRequest;
  private HttpServletResponse httpResponse;

  @Before
  public void setUp() throws IOException {
    filter = new KMSMDCFilter();
    httpRequest = Mockito.mock(HttpServletRequest.class);
    httpResponse = Mockito.mock(HttpServletResponse.class);
    KMSMDCFilter.setContext(null, null, null, null);
  }

  @Test
  public void testFilter() throws IOException, ServletException {
    when(httpRequest.getMethod()).thenReturn(METHOD);
    when(httpRequest.getRequestURL()).thenReturn(new StringBuffer(URL));
    when(httpRequest.getRemoteAddr()).thenReturn(REMOTE_ADDRESS);

    FilterChain filterChain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest request, ServletResponse response)
          throws IOException, ServletException {
        assertEquals("filter.remoteClientAddress", REMOTE_ADDRESS,
            KMSMDCFilter.getRemoteClientAddress());
        assertEquals("filter.method", METHOD, KMSMDCFilter.getMethod());
        assertEquals("filter.url", URL, KMSMDCFilter.getURL());
      }
    };

    checkMDCValuesAreEmpty();
    filter.doFilter(httpRequest, httpResponse, filterChain);
    checkMDCValuesAreEmpty();
  }

  private void checkMDCValuesAreEmpty() {
    assertNull("getRemoteClientAddress", KMSMDCFilter.getRemoteClientAddress());
    assertNull("getMethod", KMSMDCFilter.getMethod());
    assertNull("getURL", KMSMDCFilter.getURL());
    assertNull("getUgi", KMSMDCFilter.getUgi());
  }

}

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

package org.apache.hadoop.lib.servlet;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.hadoop.test.HTestCase;
import org.junit.Test;
import org.mockito.Mockito;


public class TestHostnameFilter extends HTestCase {

  @Test
  public void hostname() throws Exception {
    ServletRequest request = Mockito.mock(ServletRequest.class);
    Mockito.when(request.getRemoteAddr()).thenReturn("localhost");

    ServletResponse response = Mockito.mock(ServletResponse.class);

    final AtomicBoolean invoked = new AtomicBoolean();

    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse)
        throws IOException, ServletException {
        assertTrue(HostnameFilter.get().contains("localhost"));
        invoked.set(true);
      }
    };

    Filter filter = new HostnameFilter();
    filter.init(null);
    assertNull(HostnameFilter.get());
    filter.doFilter(request, response, chain);
    assertTrue(invoked.get());
    assertNull(HostnameFilter.get());
    filter.destroy();
  }

  @Test
  public void testMissingHostname() throws Exception {
    ServletRequest request = Mockito.mock(ServletRequest.class);
    Mockito.when(request.getRemoteAddr()).thenReturn(null);

    ServletResponse response = Mockito.mock(ServletResponse.class);

    final AtomicBoolean invoked = new AtomicBoolean();

    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse)
        throws IOException, ServletException {
        assertTrue(HostnameFilter.get().contains("???"));
        invoked.set(true);
      }
    };

    Filter filter = new HostnameFilter();
    filter.init(null);
    assertNull(HostnameFilter.get());
    filter.doFilter(request, response, chain);
    assertTrue(invoked.get());
    assertNull(HostnameFilter.get());
    filter.destroy();
  }
}

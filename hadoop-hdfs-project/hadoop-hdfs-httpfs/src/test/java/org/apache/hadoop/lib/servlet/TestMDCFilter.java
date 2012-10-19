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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.Principal;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.test.HTestCase;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.MDC;


public class TestMDCFilter extends HTestCase {

  @Test
  public void mdc() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getUserPrincipal()).thenReturn(null);
    Mockito.when(request.getMethod()).thenReturn("METHOD");
    Mockito.when(request.getPathInfo()).thenReturn("/pathinfo");

    ServletResponse response = Mockito.mock(ServletResponse.class);

    final AtomicBoolean invoked = new AtomicBoolean();

    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse)
        throws IOException, ServletException {
        assertEquals(MDC.get("hostname"), null);
        assertEquals(MDC.get("user"), null);
        assertEquals(MDC.get("method"), "METHOD");
        assertEquals(MDC.get("path"), "/pathinfo");
        invoked.set(true);
      }
    };

    MDC.clear();
    Filter filter = new MDCFilter();
    filter.init(null);

    filter.doFilter(request, response, chain);
    assertTrue(invoked.get());
    assertNull(MDC.get("hostname"));
    assertNull(MDC.get("user"));
    assertNull(MDC.get("method"));
    assertNull(MDC.get("path"));

    Mockito.when(request.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return "name";
      }
    });

    invoked.set(false);
    chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse)
        throws IOException, ServletException {
        assertEquals(MDC.get("hostname"), null);
        assertEquals(MDC.get("user"), "name");
        assertEquals(MDC.get("method"), "METHOD");
        assertEquals(MDC.get("path"), "/pathinfo");
        invoked.set(true);
      }
    };
    filter.doFilter(request, response, chain);
    assertTrue(invoked.get());

    HostnameFilter.HOSTNAME_TL.set("HOST");

    invoked.set(false);
    chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse)
        throws IOException, ServletException {
        assertEquals(MDC.get("hostname"), "HOST");
        assertEquals(MDC.get("user"), "name");
        assertEquals(MDC.get("method"), "METHOD");
        assertEquals(MDC.get("path"), "/pathinfo");
        invoked.set(true);
      }
    };
    filter.doFilter(request, response, chain);
    assertTrue(invoked.get());

    HostnameFilter.HOSTNAME_TL.remove();

    filter.destroy();
  }
}

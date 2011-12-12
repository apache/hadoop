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

import junit.framework.Assert;
import org.apache.hadoop.test.HTestCase;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.MDC;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.security.Principal;
import java.util.concurrent.atomic.AtomicBoolean;


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
        Assert.assertEquals(MDC.get("hostname"), null);
        Assert.assertEquals(MDC.get("user"), null);
        Assert.assertEquals(MDC.get("method"), "METHOD");
        Assert.assertEquals(MDC.get("path"), "/pathinfo");
        invoked.set(true);
      }
    };

    MDC.clear();
    Filter filter = new MDCFilter();
    filter.init(null);

    filter.doFilter(request, response, chain);
    Assert.assertTrue(invoked.get());
    Assert.assertNull(MDC.get("hostname"));
    Assert.assertNull(MDC.get("user"));
    Assert.assertNull(MDC.get("method"));
    Assert.assertNull(MDC.get("path"));

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
        Assert.assertEquals(MDC.get("hostname"), null);
        Assert.assertEquals(MDC.get("user"), "name");
        Assert.assertEquals(MDC.get("method"), "METHOD");
        Assert.assertEquals(MDC.get("path"), "/pathinfo");
        invoked.set(true);
      }
    };
    filter.doFilter(request, response, chain);
    Assert.assertTrue(invoked.get());

    HostnameFilter.HOSTNAME_TL.set("HOST");

    invoked.set(false);
    chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse)
        throws IOException, ServletException {
        Assert.assertEquals(MDC.get("hostname"), "HOST");
        Assert.assertEquals(MDC.get("user"), "name");
        Assert.assertEquals(MDC.get("method"), "METHOD");
        Assert.assertEquals(MDC.get("path"), "/pathinfo");
        invoked.set(true);
      }
    };
    filter.doFilter(request, response, chain);
    Assert.assertTrue(invoked.get());

    HostnameFilter.HOSTNAME_TL.remove();

    filter.destroy();
  }
}

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

package org.apache.hadoop.security.authentication.server;

import java.security.Principal;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.FilterConfig;
import javax.servlet.FilterChain;
import javax.servlet.ServletContext;
import javax.servlet.ServletResponse;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;
import org.glassfish.grizzly.servlet.HttpServletResponseImpl;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Test ProxyUserAuthenticationFilter with doAs Request Parameter.
 */
public class TestProxyUserAuthenticationFilter {

  private String actualUser;

  private static class DummyFilterConfig implements FilterConfig {
    private final Map<String, String> map;

    DummyFilterConfig(Map<String, String> map) {
      this.map = map;
    }

    @Override
    public String getFilterName() {
      return "dummy";
    }

    @Override
    public String getInitParameter(String param) {
      return map.get(param);
    }

    @Override
    public Enumeration<String> getInitParameterNames() {
      return Collections.enumeration(map.keySet());
    }

    @Override
    public ServletContext getServletContext() {
      ServletContext context = Mockito.mock(ServletContext.class);
      Mockito.when(context.getAttribute(
          AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE))
          .thenReturn(null);
      return context;
    }
  }

  private class HttpServletResponseForTest extends HttpServletResponseImpl {

  }


  @Test(timeout = 10000)
  public void testFilter() throws Exception {
    Map<String, String> params = new HashMap<String, String>();
    params.put("proxyuser.knox.users", "testuser");
    params.put("proxyuser.knox.hosts", "127.0.0.1");
    params.put("type", "simple");

    FilterConfig config = new DummyFilterConfig(params);

    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest,
          ServletResponse servletResponse) {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        actualUser = request.getRemoteUser();
      }
    };

    ProxyUserAuthenticationFilter testFilter =
        new ProxyUserAuthenticationFilter();
    testFilter.init(config);

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRemoteUser()).thenReturn("knox");
    Mockito.when(request.getParameter("doas")).thenReturn("testuser");
    Mockito.when(request.getRemoteAddr()).thenReturn("127.0.0.1");
    Mockito.when(request.getUserPrincipal()).thenReturn(new Principal() {
      @Override
      public String getName() {
        return "knox@EXAMPLE.COM";
      }
    });

    HttpServletResponseForTest response = new HttpServletResponseForTest();

    testFilter.doFilter(chain, request, response);

    assertThat(actualUser).isEqualTo("testuser");
  }


}

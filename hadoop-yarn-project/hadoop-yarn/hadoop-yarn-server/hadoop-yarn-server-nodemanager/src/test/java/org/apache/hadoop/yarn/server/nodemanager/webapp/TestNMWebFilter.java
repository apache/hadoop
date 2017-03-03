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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.inject.Injector;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.glassfish.grizzly.servlet.HttpServletResponseImpl;
import org.junit.Test;

/**
 * Basic sanity Tests for NMWebFilter.
 *
 */
public class TestNMWebFilter {

  private static final String LOG_SERVER_URI = "log-server:1999/logs";
  private static final String USER = "testUser";

  @Test(timeout = 5000)
  public void testRedirection() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(
        System.currentTimeMillis(), 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        appId, 1);
    ContainerId containerId = ContainerId.newContainerId(attemptId, 1);

    NMContext mockNMContext = mock(NMContext.class);
    ConcurrentMap<ApplicationId, Application> applications
        = new ConcurrentHashMap<>();
    when(mockNMContext.getApplications()).thenReturn(applications);
    LocalDirsHandlerService mockLocalDirsHandlerService = mock(
        LocalDirsHandlerService.class);
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    conf.set(YarnConfiguration.YARN_LOG_SERVER_URL,
        "http://" + LOG_SERVER_URI);
    when(mockLocalDirsHandlerService.getConfig()).thenReturn(conf);
    when(mockNMContext.getLocalDirsHandler()).thenReturn(
        mockLocalDirsHandlerService);
    NodeId nodeId = NodeId.newInstance("testNM", 9999);
    when(mockNMContext.getNodeId()).thenReturn(nodeId);

    Injector mockInjector = mock(Injector.class);
    NMWebAppFilter testFilter = new NMWebAppFilter(
        mockInjector, mockNMContext);

    HttpServletResponseForTest response = new HttpServletResponseForTest();
    // dummy filter
    FilterChain chain = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest,
          ServletResponse servletResponse) throws IOException,
          ServletException {
        // Do Nothing
      }
    };

    String uri = "testNM:8042/node/containerlogs/"
            + containerId.toString() + "/" + USER;
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRequestURI()).thenReturn(uri);
    testFilter.doFilter(request, response, chain);
    assertEquals(HttpServletResponse.SC_TEMPORARY_REDIRECT, response.status);
    String redirect = response.getHeader("Location");
    assertTrue(redirect.contains(LOG_SERVER_URI));
    assertTrue(redirect.contains(nodeId.toString()));
    assertTrue(redirect.contains(containerId.toString()));
    assertTrue(redirect.contains(USER));

    String logType = "syslog";
    uri = "testNM:8042/node/containerlogs/" + containerId.toString()
        + "/" + USER + "/" + logType + "/?start=10";
    HttpServletRequest request2 = mock(HttpServletRequest.class);
    when(request2.getRequestURI()).thenReturn(uri);
    when(request2.getQueryString()).thenReturn("start=10");
    testFilter.doFilter(request2, response, chain);
    assertEquals(HttpServletResponse.SC_TEMPORARY_REDIRECT, response.status);
    redirect = response.getHeader("Location");
    assertTrue(redirect.contains(LOG_SERVER_URI));
    assertTrue(redirect.contains(nodeId.toString()));
    assertTrue(redirect.contains(containerId.toString()));
    assertTrue(redirect.contains(USER));
    assertTrue(redirect.contains(logType));
    assertTrue(redirect.contains("start=10"));
  }

  private class HttpServletResponseForTest extends HttpServletResponseImpl {
    String redirectLocation = "";
    int status;
    private String contentType;
    private final Map<String, String> headers = new HashMap<>(1);
    private StringWriter body;

    public String getRedirect() {
      return redirectLocation;
    }

    @Override
    public void sendRedirect(String location) throws IOException {
      redirectLocation = location;
    }

    @Override
    public String encodeRedirectURL(String url) {
      return url;
    }

    @Override
    public void setStatus(int status) {
      this.status = status;
    }

    @Override
    public void setContentType(String type) {
      this.contentType = type;
    }

    @Override
    public void setHeader(String name, String value) {
      headers.put(name, value);
    }

    public String getHeader(String name) {
      return headers.get(name);
    }

    @Override
    public PrintWriter getWriter() throws IOException {
      body = new StringWriter();
      return new PrintWriter(body);
    }
  }
}

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
package org.apache.hadoop.yarn.server.router;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.http.CrossOriginFilter;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.webapp.WebAppContext;
import org.glassfish.grizzly.servlet.HttpServletResponseImpl;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests {@link Router}.
 */
public class TestRouter {

  @Test
  public void testJVMMetricsService() {
    YarnConfiguration conf = new YarnConfiguration();
    Router router = new Router();
    router.init(conf);
    assertEquals(3, router.getServices().size());
  }

  @Test
  public void testServiceACLRefresh() {
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        true);
    String aclsString = "alice,bob users,wheel";
    conf.set("security.applicationclient.protocol.acl", aclsString);
    conf.set("security.resourcemanager-administration.protocol.acl",
        aclsString);

    Router router = new Router();
    router.init(conf);
    router.start();

    // verify service Acls refresh for RouterClientRMService
    ServiceAuthorizationManager clientRMServiceManager =
        router.clientRMProxyService.getServer().
        getServiceAuthorizationManager();
    verifyServiceACLsRefresh(clientRMServiceManager,
        org.apache.hadoop.yarn.api.ApplicationClientProtocolPB.class,
        aclsString);

    // verify service Acls refresh for RouterRMAdminService
    ServiceAuthorizationManager routerAdminServiceManager =
        router.rmAdminProxyService.getServer().getServiceAuthorizationManager();
    verifyServiceACLsRefresh(routerAdminServiceManager,
        org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocolPB.class,
        aclsString);

    router.stop();

  }

  private void verifyServiceACLsRefresh(ServiceAuthorizationManager manager,
      Class<?> protocol, String aclString) {
    if (manager.getProtocolsWithAcls().size() == 0) {
      fail("Acls are not refreshed for protocol " + protocol);
    }
    for (Class<?> protocolClass : manager.getProtocolsWithAcls()) {
      AccessControlList accessList = manager.getProtocolsAcls(protocolClass);
      if (protocolClass == protocol) {
        Assert.assertEquals(accessList.getAclString(), aclString);
      }
    }
  }

  @Test
  public void testRouterSupportCrossOrigin() throws ServletException, IOException {

    // We design test cases like this
    // We start the Router and enable the Router to support Cross-origin.
    // In the configuration, we allow example.com to access.
    // 1. We simulate example.com and get the correct response
    // 2. We simulate example.org and cannot get a response

    // Initialize RouterWeb's CrossOrigin capability
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.ROUTER_WEBAPP_ENABLE_CORS_FILTER, true);
    conf.set("hadoop.http.filter.initializers", HttpCrossOriginFilterInitializer.class.getName());
    conf.set(HttpCrossOriginFilterInitializer.PREFIX + CrossOriginFilter.ALLOWED_ORIGINS,
        "example.com");
    conf.set(HttpCrossOriginFilterInitializer.PREFIX + CrossOriginFilter.ALLOWED_HEADERS,
        "X-Requested-With,Accept");
    conf.set(HttpCrossOriginFilterInitializer.PREFIX + CrossOriginFilter.ALLOWED_METHODS,
        "GET,POST");

    // Start the router
    Router router = new Router();
    router.init(conf);
    router.start();
    router.getServices();

    // Get assigned to Filter.
    // The name of the filter is "Cross Origin Filter",
    // which is specified in HttpCrossOriginFilterInitializer.
    WebApp webApp = router.getWebapp();
    HttpServer2 httpServer2 = webApp.getHttpServer();
    WebAppContext webAppContext = httpServer2.getWebAppContext();
    ServletHandler servletHandler = webAppContext.getServletHandler();
    FilterHolder holder = servletHandler.getFilter("Cross Origin Filter");
    CrossOriginFilter filter = CrossOriginFilter.class.cast(holder.getFilter());

    // 1. Simulate [example.com] for access
    HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq.getHeader("Origin")).thenReturn("example.com");
    Mockito.when(mockReq.getHeader("Access-Control-Request-Method")).thenReturn("GET");
    Mockito.when(mockReq.getHeader("Access-Control-Request-Headers"))
        .thenReturn("X-Requested-With");

    // Objects to verify interactions based on request
    HttpServletResponseForRouterTest mockRes = new HttpServletResponseForRouterTest();
    FilterChain mockChain = Mockito.mock(FilterChain.class);

    // Object under test
    filter.doFilter(mockReq, mockRes, mockChain);

    // Why is 5, because when Filter passes,
    // CrossOriginFilter will set 5 values to Map
    Assert.assertEquals(5, mockRes.getHeaders().size());
    String allowResult = mockRes.getHeader("Access-Control-Allow-Credentials");
    Assert.assertEquals("true", allowResult);

    // 2. Simulate [example.org] for access
    HttpServletRequest mockReq2 = Mockito.mock(HttpServletRequest.class);
    Mockito.when(mockReq2.getHeader("Origin")).thenReturn("example.org");
    Mockito.when(mockReq2.getHeader("Access-Control-Request-Method")).thenReturn("GET");
    Mockito.when(mockReq2.getHeader("Access-Control-Request-Headers"))
        .thenReturn("X-Requested-With");

    // Objects to verify interactions based on request
    HttpServletResponseForRouterTest mockRes2 = new HttpServletResponseForRouterTest();
    FilterChain mockChain2 = Mockito.mock(FilterChain.class);

    // Object under test
    filter.doFilter(mockReq2, mockRes2, mockChain2);

    // Why is 0, because when the Filter fails,
    // CrossOriginFilter will not set any value
    Assert.assertEquals(0, mockRes2.getHeaders().size());

    router.stop();
  }

  private class HttpServletResponseForRouterTest extends HttpServletResponseImpl {
    private final Map<String, String> headers = new HashMap<>(1);
    @Override
    public void setHeader(String name, String value) {
      headers.put(name, value);
    }

    public String getHeader(String name) {
      return headers.get(name);
    }

    public Map<String, String> getHeaders() {
      return headers;
    }
  }

}

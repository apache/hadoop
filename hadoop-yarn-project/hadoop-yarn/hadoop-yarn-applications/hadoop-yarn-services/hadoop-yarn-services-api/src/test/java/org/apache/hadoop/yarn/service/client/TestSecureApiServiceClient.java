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

package org.apache.hadoop.yarn.service.client;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import javax.security.sasl.Sasl;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.Map;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test Spnego Client Login.
 */
public class TestSecureApiServiceClient extends KerberosSecurityTestcase {

  private String clientPrincipal = "client";

  private String server1Protocol = "HTTP";

  private String server2Protocol = "server2";

  private String host = "localhost";

  private String server1Principal = server1Protocol + "/" + host;

  private String server2Principal = server2Protocol + "/" + host;

  private File keytabFile;

  private Configuration testConf = new Configuration();

  private Map<String, String> props;
  private static Server server;
  private static Logger LOG = Logger
      .getLogger(TestSecureApiServiceClient.class);
  private ApiServiceClient asc;

  /**
   * A mocked version of API Service for testing purpose.
   *
   */
  @SuppressWarnings("serial")
  public static class TestServlet extends HttpServlet {

    private static boolean headerFound = false;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      Enumeration<String> headers = req.getHeaderNames();
      while(headers.hasMoreElements()) {
        String header = headers.nextElement();
        LOG.info(header);
      }
      if (req.getHeader("Authorization")!=null) {
        headerFound = true;
        resp.setStatus(HttpServletResponse.SC_OK);
      } else {
        headerFound = false;
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
      }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      resp.setStatus(HttpServletResponse.SC_OK);
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      resp.setStatus(HttpServletResponse.SC_OK);
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      resp.setStatus(HttpServletResponse.SC_OK);
    }

    public static boolean isHeaderExist() {
      return headerFound;
    }
  }

  @Before
  public void setUp() throws Exception {
    keytabFile = new File(getWorkDir(), "keytab");
    getKdc().createPrincipal(keytabFile, clientPrincipal, server1Principal,
        server2Principal);
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS,
        testConf);
    UserGroupInformation.setConfiguration(testConf);
    UserGroupInformation.setShouldRenewImmediatelyForTests(true);
    props = new HashMap<String, String>();
    props.put(Sasl.QOP, QualityOfProtection.AUTHENTICATION.saslQop);
    server = new Server(8088);
    ((QueuedThreadPool)server.getThreadPool()).setMaxThreads(10);
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/app");
    server.setHandler(context);
    context.addServlet(new ServletHolder(TestServlet.class), "/*");
    ((ServerConnector)server.getConnectors()[0]).setHost("localhost");
    server.start();

    List<String> rmServers = new ArrayList<String>();
    rmServers.add("localhost:8088");
    testConf.set("yarn.resourcemanager.webapp.address",
        "localhost:8088");
    asc = new ApiServiceClient() {
      @Override
      List<String> getRMHAWebAddresses(Configuration conf) {
        return rmServers;
      }
    };
    asc.serviceInit(testConf);
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  @Test
  public void testHttpSpnegoChallenge() throws Exception {
    UserGroupInformation.loginUserFromKeytab(clientPrincipal, keytabFile
        .getCanonicalPath());
    asc = new ApiServiceClient();
    String challenge = asc.generateToken("localhost");
    assertNotNull(challenge);
  }

  @Test
  public void testAuthorizationHeader() throws Exception {
    UserGroupInformation.loginUserFromKeytab(clientPrincipal, keytabFile
        .getCanonicalPath());
    String rmAddress = asc.getRMWebAddress();
    if (TestServlet.isHeaderExist()) {
      assertEquals(rmAddress, "http://localhost:8088");
    } else {
      fail("Did not see Authorization header.");
    }
  }
}

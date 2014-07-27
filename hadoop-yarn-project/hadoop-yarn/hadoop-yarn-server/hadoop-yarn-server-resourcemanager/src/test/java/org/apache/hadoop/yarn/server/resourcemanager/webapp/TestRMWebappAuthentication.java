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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.sun.jersey.api.client.ClientResponse.Status;

/* Just a simple test class to ensure that the RM handles the static web user
 * correctly for secure and un-secure modes
 * 
 */
@RunWith(Parameterized.class)
public class TestRMWebappAuthentication {

  private static MockRM rm;
  private static Configuration simpleConf;
  private static Configuration kerberosConf;

  private static final File testRootDir = new File("target",
    TestRMWebServicesDelegationTokenAuthentication.class.getName() + "-root");
  private static File httpSpnegoKeytabFile = new File(
    KerberosTestUtils.getKeytabFile());

  private static boolean miniKDCStarted = false;
  private static MiniKdc testMiniKDC;

  static {
    simpleConf = new Configuration();
    simpleConf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    simpleConf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
      ResourceScheduler.class);
    simpleConf.setBoolean("mockrm.webapp.enabled", true);
    kerberosConf = new Configuration();
    kerberosConf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    kerberosConf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
      ResourceScheduler.class);
    kerberosConf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    kerberosConf.set(
      CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    kerberosConf.set(YarnConfiguration.RM_KEYTAB,
      httpSpnegoKeytabFile.getAbsolutePath());
    kerberosConf.setBoolean("mockrm.webapp.enabled", true);
  }

  @Parameters
  public static Collection params() {
    return Arrays.asList(new Object[][] { { 1, simpleConf },
        { 2, kerberosConf } });
  }

  public TestRMWebappAuthentication(int run, Configuration conf) {
    super();
    setupAndStartRM(conf);
  }

  @BeforeClass
  public static void setUp() {
    try {
      testMiniKDC = new MiniKdc(MiniKdc.createConf(), testRootDir);
      setupKDC();
    } catch (Exception e) {
      assertTrue("Couldn't create MiniKDC", false);
    }
  }

  @AfterClass
  public static void tearDown() {
    if (testMiniKDC != null) {
      testMiniKDC.stop();
    }
  }

  private static void setupKDC() throws Exception {
    if (!miniKDCStarted) {
      testMiniKDC.start();
      getKdc().createPrincipal(httpSpnegoKeytabFile, "HTTP/localhost",
        "client", UserGroupInformation.getLoginUser().getShortUserName());
      miniKDCStarted = true;
    }
  }

  private static MiniKdc getKdc() {
    return testMiniKDC;
  }

  private static void setupAndStartRM(Configuration conf) {
    UserGroupInformation.setConfiguration(conf);
    rm = new MockRM(conf);
  }

  // ensure that in a non-secure cluster users can access
  // the web pages as earlier and submit apps as anonymous
  // user or by identifying themselves
  @Test
  public void testSimpleAuth() throws Exception {

    rm.start();

    // ensure users can access web pages
    // this should work for secure and non-secure clusters
    URL url = new URL("http://localhost:8088/cluster");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    try {
      conn.getInputStream();
      assertEquals(Status.OK.getStatusCode(), conn.getResponseCode());
    } catch (Exception e) {
      fail("Fetching url failed");
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      testAnonymousKerberosUser();
    } else {
      testAnonymousSimpleUser();
    }

    rm.stop();
  }

  private void testAnonymousKerberosUser() throws Exception {

    ApplicationSubmissionContextInfo app =
        new ApplicationSubmissionContextInfo();
    String appid = "application_123_0";
    app.setApplicationId(appid);
    String requestBody =
        TestRMWebServicesDelegationTokenAuthentication
          .getMarshalledAppInfo(app);

    URL url =
        new URL("http://localhost:8088/ws/v1/cluster/apps/new-application");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    TestRMWebServicesDelegationTokenAuthentication.setupConn(conn, "POST",
      "application/xml", requestBody);

    try {
      conn.getInputStream();
      fail("Anonymous users should not be allowed to get new application ids in secure mode.");
    } catch (IOException ie) {
      assertEquals(Status.FORBIDDEN.getStatusCode(), conn.getResponseCode());
    }

    url = new URL("http://localhost:8088/ws/v1/cluster/apps");
    conn = (HttpURLConnection) url.openConnection();
    TestRMWebServicesDelegationTokenAuthentication.setupConn(conn, "POST",
      "application/xml", requestBody);

    try {
      conn.getInputStream();
      fail("Anonymous users should not be allowed to submit apps in secure mode.");
    } catch (IOException ie) {
      assertEquals(Status.FORBIDDEN.getStatusCode(), conn.getResponseCode());
    }

    requestBody = "{ \"state\": \"KILLED\"}";
    url =
        new URL(
          "http://localhost:8088/ws/v1/cluster/apps/application_123_0/state");
    conn = (HttpURLConnection) url.openConnection();
    TestRMWebServicesDelegationTokenAuthentication.setupConn(conn, "PUT",
      "application/json", requestBody);

    try {
      conn.getInputStream();
      fail("Anonymous users should not be allowed to kill apps in secure mode.");
    } catch (IOException ie) {
      assertEquals(Status.FORBIDDEN.getStatusCode(), conn.getResponseCode());
    }
  }

  private void testAnonymousSimpleUser() throws Exception {

    ApplicationSubmissionContextInfo app =
        new ApplicationSubmissionContextInfo();
    String appid = "application_123_0";
    app.setApplicationId(appid);
    String requestBody =
        TestRMWebServicesDelegationTokenAuthentication
          .getMarshalledAppInfo(app);

    URL url = new URL("http://localhost:8088/ws/v1/cluster/apps");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    TestRMWebServicesDelegationTokenAuthentication.setupConn(conn, "POST",
      "application/xml", requestBody);

    conn.getInputStream();
    assertEquals(Status.ACCEPTED.getStatusCode(), conn.getResponseCode());
    boolean appExists =
        rm.getRMContext().getRMApps()
          .containsKey(ConverterUtils.toApplicationId(appid));
    assertTrue(appExists);
    RMApp actualApp =
        rm.getRMContext().getRMApps()
          .get(ConverterUtils.toApplicationId(appid));
    String owner = actualApp.getUser();
    assertEquals(
      rm.getConfig().get(CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER,
        CommonConfigurationKeys.DEFAULT_HADOOP_HTTP_STATIC_USER), owner);

    appid = "application_123_1";
    app.setApplicationId(appid);
    requestBody =
        TestRMWebServicesDelegationTokenAuthentication
          .getMarshalledAppInfo(app);
    url = new URL("http://localhost:8088/ws/v1/cluster/apps?user.name=client");
    conn = (HttpURLConnection) url.openConnection();
    TestRMWebServicesDelegationTokenAuthentication.setupConn(conn, "POST",
      MediaType.APPLICATION_XML, requestBody);

    conn.getInputStream();
    appExists =
        rm.getRMContext().getRMApps()
          .containsKey(ConverterUtils.toApplicationId(appid));
    assertTrue(appExists);
    actualApp =
        rm.getRMContext().getRMApps()
          .get(ConverterUtils.toApplicationId(appid));
    owner = actualApp.getUser();
    assertEquals("client", owner);

  }

}

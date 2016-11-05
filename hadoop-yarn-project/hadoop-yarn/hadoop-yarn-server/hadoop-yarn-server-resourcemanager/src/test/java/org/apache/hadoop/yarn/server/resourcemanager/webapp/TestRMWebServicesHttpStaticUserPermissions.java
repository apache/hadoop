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
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.api.client.ClientResponse.Status;

public class TestRMWebServicesHttpStaticUserPermissions {

  private static final File testRootDir = new File("target",
    TestRMWebServicesHttpStaticUserPermissions.class.getName() + "-root");
  private static File spnegoKeytabFile = new File(
    KerberosTestUtils.getKeytabFile());

  private static String spnegoPrincipal = KerberosTestUtils
    .getServerPrincipal();

  private static MiniKdc testMiniKDC;
  private static MockRM rm;

  static class Helper {
    String method;
    String requestBody;

    Helper(String method, String requestBody) {
      this.method = method;
      this.requestBody = requestBody;
    }
  }

  @BeforeClass
  public static void setUp() {
    try {
      testMiniKDC = new MiniKdc(MiniKdc.createConf(), testRootDir);
      setupKDC();
      setupAndStartRM();
    } catch (Exception e) {
      fail("Couldn't create MiniKDC");
    }
  }

  @AfterClass
  public static void tearDown() {
    if (testMiniKDC != null) {
      testMiniKDC.stop();
    }
    if (rm != null) {
      rm.stop();
    }
  }

  public TestRMWebServicesHttpStaticUserPermissions() throws Exception {
    super();
  }

  private static void setupAndStartRM() throws Exception {
    Configuration rmconf = new Configuration();
    rmconf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    rmconf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
      ResourceScheduler.class);
    rmconf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    rmconf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      "kerberos");
    rmconf.set(YarnConfiguration.RM_PRINCIPAL, spnegoPrincipal);
    rmconf.set(YarnConfiguration.RM_KEYTAB,
        spnegoKeytabFile.getAbsolutePath());
    rmconf.setBoolean("mockrm.webapp.enabled", true);
    UserGroupInformation.setConfiguration(rmconf);
    rm = new MockRM(rmconf);
    rm.start();

  }

  private static void setupKDC() throws Exception {
    testMiniKDC.start();
    testMiniKDC.createPrincipal(spnegoKeytabFile, "HTTP/localhost", "client",
      UserGroupInformation.getLoginUser().getShortUserName(), "client2");
  }

  // Test that the http static user can't submit or kill apps
  // when secure mode is turned on

  @Test
  public void testWebServiceAccess() throws Exception {

    ApplicationSubmissionContextInfo app =
        new ApplicationSubmissionContextInfo();
    String appid = "application_123_0";
    app.setApplicationId(appid);
    String submitAppRequestBody =
        TestRMWebServicesDelegationTokenAuthentication
          .getMarshalledAppInfo(app);

    URL url = new URL("http://localhost:8088/ws/v1/cluster/apps");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    // we should be access the apps page with the static user
    TestRMWebServicesDelegationTokenAuthentication.setupConn(conn, "GET", "",
      "");
    try {
      conn.getInputStream();
      assertEquals(Status.OK.getStatusCode(), conn.getResponseCode());
    } catch (IOException e) {
      fail("Got " + conn.getResponseCode() + " instead of 200 accessing "
          + url.toString());
    }
    conn.disconnect();

    // new-application, submit app and kill should fail with
    // forbidden
    Map<String, Helper> urlRequestMap = new HashMap<String, Helper>();
    String killAppRequestBody =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
            + "<appstate>\n" + "  <state>KILLED</state>\n" + "</appstate>";

    urlRequestMap.put("http://localhost:8088/ws/v1/cluster/apps", new Helper(
      "POST", submitAppRequestBody));
    urlRequestMap.put(
      "http://localhost:8088/ws/v1/cluster/apps/new-application", new Helper(
        "POST", ""));
    urlRequestMap.put(
      "http://localhost:8088/ws/v1/cluster/apps/app_123_1/state", new Helper(
        "PUT", killAppRequestBody));

    for (Map.Entry<String, Helper> entry : urlRequestMap.entrySet()) {
      URL reqURL = new URL(entry.getKey());
      conn = (HttpURLConnection) reqURL.openConnection();
      String method = entry.getValue().method;
      String body = entry.getValue().requestBody;
      TestRMWebServicesDelegationTokenAuthentication.setupConn(conn, method,
        "application/xml", body);
      try {
        conn.getInputStream();
        fail("Request " + entry.getKey() + "succeeded but should have failed");
      } catch (IOException e) {
        assertEquals(Status.FORBIDDEN.getStatusCode(), conn.getResponseCode());
        InputStream errorStream = conn.getErrorStream();
        String error = "";
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(errorStream, "UTF8"));
        for (String line; (line = reader.readLine()) != null;) {
          error += line;
        }
        reader.close();
        errorStream.close();
        assertEquals(
          "The default static user cannot carry out this operation.", error);
      }
      conn.disconnect();
    }
  }
}

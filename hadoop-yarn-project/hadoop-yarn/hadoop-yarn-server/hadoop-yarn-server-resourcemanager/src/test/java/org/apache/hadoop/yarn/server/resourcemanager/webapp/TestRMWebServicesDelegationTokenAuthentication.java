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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.api.client.ClientResponse.Status;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_PROXY_USER_PREFIX;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestRMWebServicesDelegationTokenAuthentication {

  private static final File testRootDir = new File("target",
    TestRMWebServicesDelegationTokenAuthentication.class.getName() + "-root");
  private static File httpSpnegoKeytabFile = new File(
    KerberosTestUtils.getKeytabFile());

  private static String httpSpnegoPrincipal = KerberosTestUtils
    .getServerPrincipal();

  private static boolean miniKDCStarted = false;
  private static MiniKdc testMiniKDC;
  private static MockRM rm;


  String delegationTokenHeader;

  // use published header name
  final static String OldDelegationTokenHeader =
      "Hadoop-YARN-Auth-Delegation-Token";

  // alternate header name
  final static String NewDelegationTokenHeader =
      DelegationTokenAuthenticator.DELEGATION_TOKEN_HEADER;

  @BeforeClass
  public static void setUp() {
    try {
      testMiniKDC = new MiniKdc(MiniKdc.createConf(), testRootDir);
      setupKDC();
      setupAndStartRM();
    } catch (Exception e) {
      assertTrue("Couldn't create MiniKDC", false);
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

  @Parameterized.Parameters
  public static Collection<Object[]> headers() {
    return Arrays.asList(new Object[][] { {OldDelegationTokenHeader}, {NewDelegationTokenHeader}});
  }

  public TestRMWebServicesDelegationTokenAuthentication(String header) throws Exception {
    super();
    this.delegationTokenHeader = header;
  }

  private static void setupAndStartRM() throws Exception {
    Configuration rmconf = new Configuration();
    rmconf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    rmconf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
      ResourceScheduler.class);
    rmconf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    String httpPrefix = "hadoop.http.authentication.";
    rmconf.setStrings(httpPrefix + "type", "kerberos");
    rmconf.set(httpPrefix + KerberosAuthenticationHandler.PRINCIPAL,
      httpSpnegoPrincipal);
    rmconf.set(httpPrefix + KerberosAuthenticationHandler.KEYTAB,
      httpSpnegoKeytabFile.getAbsolutePath());
    // use any file for signature secret
    rmconf.set(httpPrefix + AuthenticationFilter.SIGNATURE_SECRET + ".file",
      httpSpnegoKeytabFile.getAbsolutePath());
    rmconf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      "kerberos");
    rmconf.setBoolean(YarnConfiguration.RM_WEBAPP_DELEGATION_TOKEN_AUTH_FILTER,
      true);
    rmconf.set("hadoop.http.filter.initializers",
      AuthenticationFilterInitializer.class.getName());
    rmconf.set(YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY,
      httpSpnegoPrincipal);
    rmconf.set(YarnConfiguration.RM_KEYTAB,
      httpSpnegoKeytabFile.getAbsolutePath());
    rmconf.set(YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY,
      httpSpnegoKeytabFile.getAbsolutePath());
    rmconf.set(YarnConfiguration.NM_WEBAPP_SPNEGO_USER_NAME_KEY,
      httpSpnegoPrincipal);
    rmconf.set(YarnConfiguration.NM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY,
      httpSpnegoKeytabFile.getAbsolutePath());
    rmconf.setBoolean("mockrm.webapp.enabled", true);
    rmconf.set(RM_PROXY_USER_PREFIX + "client.hosts", "*");
    rmconf.set(RM_PROXY_USER_PREFIX + "client.groups", "*");
    UserGroupInformation.setConfiguration(rmconf);
    rm = new MockRM(rmconf);
    rm.start();

  }

  private static void setupKDC() throws Exception {
    if (!miniKDCStarted) {
      testMiniKDC.start();
      getKdc().createPrincipal(httpSpnegoKeytabFile, "HTTP/localhost",
        "client", UserGroupInformation.getLoginUser().getShortUserName(),
        "client2");
      miniKDCStarted = true;
    }
  }

  private static MiniKdc getKdc() {
    return testMiniKDC;
  }

  // Test that you can authenticate with only delegation tokens
  // 1. Get a delegation token using Kerberos auth(this ends up
  // testing the fallback authenticator)
  // 2. Submit an app without kerberos or delegation-token
  // - we should get an UNAUTHORIZED response
  // 3. Submit same app with delegation-token
  // - we should get OK response
  // - confirm owner of the app is the user whose
  // delegation-token we used

  @Test
  public void testDelegationTokenAuth() throws Exception {
    final String token = getDelegationToken("test");

    ApplicationSubmissionContextInfo app =
        new ApplicationSubmissionContextInfo();
    String appid = "application_123_0";
    app.setApplicationId(appid);
    String requestBody = getMarshalledAppInfo(app);

    URL url = new URL("http://localhost:8088/ws/v1/cluster/apps");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    setupConn(conn, "POST", "application/xml", requestBody);

    // this should fail with unauthorized because only
    // auth is kerberos or delegation token
    try {
      conn.getInputStream();
      fail("we should not be here");
    } catch (IOException e) {
      assertEquals(Status.UNAUTHORIZED.getStatusCode(), conn.getResponseCode());
    }

    conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty(delegationTokenHeader, token);
    setupConn(conn, "POST", MediaType.APPLICATION_XML, requestBody);

    // this should not fail
    try {
      conn.getInputStream();
    }
    catch(IOException ie) {
      InputStream errorStream = conn.getErrorStream();
      String error = "";
      BufferedReader reader = null;
      reader = new BufferedReader(new InputStreamReader(errorStream, "UTF8"));
      for (String line; (line = reader.readLine()) != null;) {
        error += line;
      }
      reader.close();
      errorStream.close();
      fail("Response " + conn.getResponseCode() + "; " + error);
    }

    boolean appExists =
        rm.getRMContext().getRMApps()
          .containsKey(ApplicationId.fromString(appid));
    assertTrue(appExists);
    RMApp actualApp =
        rm.getRMContext().getRMApps()
          .get(ApplicationId.fromString(appid));
    String owner = actualApp.getUser();
    assertEquals("client", owner);
  }

  // Test to make sure that cancelled delegation tokens
  // are rejected
  @Test
  public void testCancelledDelegationToken() throws Exception {
    String token = getDelegationToken("client");
    cancelDelegationToken(token);
    ApplicationSubmissionContextInfo app =
        new ApplicationSubmissionContextInfo();
    String appid = "application_123_0";
    app.setApplicationId(appid);
    String requestBody = getMarshalledAppInfo(app);

    URL url = new URL("http://localhost:8088/ws/v1/cluster/apps");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty(delegationTokenHeader, token);
    setupConn(conn, "POST", MediaType.APPLICATION_XML, requestBody);

    // this should fail with unauthorized because only
    // auth is kerberos or delegation token
    try {
      conn.getInputStream();
      fail("Authentication should fail with expired delegation tokens");
    } catch (IOException e) {
      assertEquals(Status.FORBIDDEN.getStatusCode(), conn.getResponseCode());
    }
  }

  // Test to make sure that we can't do delegation token
  // functions using just delegation token auth
  @Test
  public void testDelegationTokenOps() throws Exception {
    String token = getDelegationToken("client");
    String createRequest = "{\"renewer\":\"test\"}";
    String renewRequest = "{\"token\": \"" + token + "\"}";

    // first test create and renew
    String[] requests = { createRequest, renewRequest };
    for (String requestBody : requests) {
      URL url = new URL("http://localhost:8088/ws/v1/cluster/delegation-token");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty(delegationTokenHeader, token);
      setupConn(conn, "POST", MediaType.APPLICATION_JSON, requestBody);
      try {
        conn.getInputStream();
        fail("Creation/Renewing delegation tokens should not be "
            + "allowed with token auth");
      } catch (IOException e) {
        assertEquals(Status.FORBIDDEN.getStatusCode(), conn.getResponseCode());
      }
    }

    // test cancel
    URL url = new URL("http://localhost:8088/ws/v1/cluster/delegation-token");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty(delegationTokenHeader, token);
    conn.setRequestProperty(RMWebServices.DELEGATION_TOKEN_HEADER, token);
    setupConn(conn, "DELETE", null, null);
    try {
      conn.getInputStream();
      fail("Cancelling delegation tokens should not be allowed with token auth");
    } catch (IOException e) {
      assertEquals(Status.FORBIDDEN.getStatusCode(), conn.getResponseCode());
    }
  }

  // Superuser "client" should be able to get a delegation token
  // for user "client2" when authenticated using Kerberos
  // The request shouldn't work when authenticated using DelegationTokens
  @Test
  public void testDoAs() throws Exception {

    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        String token = "";
        String owner = "";
        String renewer = "renewer";
        String body = "{\"renewer\":\"" + renewer + "\"}";
        URL url =
            new URL("http://localhost:8088/ws/v1/cluster/delegation-token?doAs=client2");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        setupConn(conn, "POST", MediaType.APPLICATION_JSON, body);
        InputStream response = conn.getInputStream();
        assertEquals(Status.OK.getStatusCode(), conn.getResponseCode());
        BufferedReader reader = null;
        try {
          reader = new BufferedReader(new InputStreamReader(response, "UTF8"));
          for (String line; (line = reader.readLine()) != null;) {
            JSONObject obj = new JSONObject(line);
            if (obj.has("token")) {
              token = obj.getString("token");
            }
            if(obj.has("owner")) {
              owner = obj.getString("owner");
            }
          }
        } finally {
          IOUtils.closeQuietly(reader);
          IOUtils.closeQuietly(response);
        }
        Assert.assertEquals("client2", owner);
        Token<RMDelegationTokenIdentifier> realToken = new Token<RMDelegationTokenIdentifier>();
        realToken.decodeFromUrlString(token);
        Assert.assertEquals("client2", realToken.decodeIdentifier().getOwner().toString());
        return null;
      }
    });

    // this should not work
    final String token = getDelegationToken("client");
    String renewer = "renewer";
    String body = "{\"renewer\":\"" + renewer + "\"}";
    URL url =
        new URL("http://localhost:8088/ws/v1/cluster/delegation-token?doAs=client2");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty(delegationTokenHeader, token);
    setupConn(conn, "POST", MediaType.APPLICATION_JSON, body);
    try {
      conn.getInputStream();
      fail("Client should not be allowed to impersonate using delegation tokens");
    }
    catch(IOException ie) {
      assertEquals(Status.FORBIDDEN.getStatusCode(), conn.getResponseCode());
    }

    // this should also fail due to client2 not being a super user
    KerberosTestUtils.doAs("client2@EXAMPLE.COM", new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        String renewer = "renewer";
        String body = "{\"renewer\":\"" + renewer + "\"}";
        URL url =
            new URL(
                "http://localhost:8088/ws/v1/cluster/delegation-token?doAs=client");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        setupConn(conn, "POST", MediaType.APPLICATION_JSON, body);
        try {
          conn.getInputStream();
          fail("Non superuser client should not be allowed to carry out doAs");
        }
        catch (IOException ie) {
          assertEquals(Status.FORBIDDEN.getStatusCode(), conn.getResponseCode());
        }
        return null;
      }
    });

  }

  private String getDelegationToken(final String renewer) throws Exception {
    return KerberosTestUtils.doAsClient(new Callable<String>() {
      @Override
      public String call() throws Exception {
        String ret = null;
        String body = "{\"renewer\":\"" + renewer + "\"}";
        URL url =
            new URL("http://localhost:8088/ws/v1/cluster/delegation-token");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        setupConn(conn, "POST", MediaType.APPLICATION_JSON, body);
        InputStream response = conn.getInputStream();
        assertEquals(Status.OK.getStatusCode(), conn.getResponseCode());
        BufferedReader reader = null;
        try {
          reader = new BufferedReader(new InputStreamReader(response, "UTF8"));
          for (String line; (line = reader.readLine()) != null;) {
            JSONObject obj = new JSONObject(line);
            if (obj.has("token")) {
              reader.close();
              response.close();
              ret = obj.getString("token");
              break;
            }
          }
        } finally {
          IOUtils.closeQuietly(reader);
          IOUtils.closeQuietly(response);
        }
        return ret;
      }
    });
  }

  private void cancelDelegationToken(final String tokenString) throws Exception {

    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        URL url =
            new URL("http://localhost:8088/ws/v1/cluster/delegation-token");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty(RMWebServices.DELEGATION_TOKEN_HEADER,
          tokenString);
        setupConn(conn, "DELETE", null, null);
        InputStream response = conn.getInputStream();
        assertEquals(Status.OK.getStatusCode(), conn.getResponseCode());
        response.close();
        return null;
      }
    });
  }

  static String getMarshalledAppInfo(ApplicationSubmissionContextInfo appInfo)
      throws Exception {

    StringWriter writer = new StringWriter();
    JAXBContext context =
        JAXBContext.newInstance(ApplicationSubmissionContextInfo.class);
    Marshaller m = context.createMarshaller();
    m.marshal(appInfo, writer);
    return writer.toString();
  }

  static void setupConn(HttpURLConnection conn, String method,
      String contentType, String body) throws Exception {
    conn.setRequestMethod(method);
    conn.setDoOutput(true);
    conn.setRequestProperty("Accept-Charset", "UTF8");
    if (contentType != null && !contentType.isEmpty()) {
      conn.setRequestProperty("Content-Type", contentType + ";charset=UTF8");
      if (body != null && !body.isEmpty()) {
        OutputStream stream = conn.getOutputStream();
        stream.write(body.getBytes("UTF8"));
        stream.close();
      }
    }
  }
}

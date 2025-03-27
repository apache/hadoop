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

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.createRM;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.client.Entity;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.DelegationToken;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.toJson;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.toEntity;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.logging.LoggingFeature;
import org.glassfish.jersey.server.ResourceConfig;

public class TestRMWebServicesDelegationTokens extends JerseyTestBase {

  private static File testRootDir;
  private static File httpSpnegoKeytabFile = new File(
    KerberosTestUtils.getKeytabFile());
  private static String httpSpnegoPrincipal = KerberosTestUtils
    .getServerPrincipal();
  private static MiniKdc testMiniKDC;
  private static MockRM rm;
  private boolean isKerberosAuth = false;
  private ResourceConfig config;
  private HttpServletRequest request = mock(HttpServletRequest.class);

  @Override
  protected Application configure() {
    config = new ResourceConfig();
    config.register(RMWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(TestRMWebServicesAppsModification.TestRMCustomAuthFilter.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    return config;
  }

  private class JerseyBinder extends AbstractBinder {
    private Configuration conf = new YarnConfiguration();

    @Override
    protected void configure() {
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
          ResourceScheduler.class);
      conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
      rm = createRM(conf);
      configureScheduler();
      when(request.getScheme()).thenReturn("http");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(conf).to(Configuration.class).named("conf");
      bind(request).to(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);

      Principal principal1 = () -> "testuser";
      when(request.getUserPrincipal()).thenReturn(principal1);

      if (isKerberosAuth) {
        bind(TestKerberosAuthFilter.class);
      } else {
        bind(TestSimpleAuthFilter.class);
      }
    }

    public void configureScheduler() {
    }

    public Configuration getConf() {
      return conf;
    }
  }

  // Make sure the test uses the published header string
  final String yarnTokenHeader = "Hadoop-YARN-RM-Delegation-Token";

  public static class TestKerberosAuthFilter extends AuthenticationFilter {
    @Override
    protected Properties getConfiguration(String configPrefix,
        FilterConfig filterConfig) throws ServletException {

      Properties properties =
          super.getConfiguration(configPrefix, filterConfig);

      properties.put(KerberosAuthenticationHandler.PRINCIPAL,
        httpSpnegoPrincipal);
      properties.put(KerberosAuthenticationHandler.KEYTAB,
        httpSpnegoKeytabFile.getAbsolutePath());
      properties.put(AuthenticationFilter.AUTH_TYPE, "kerberos");
      return properties;
    }
  }

  public static class TestSimpleAuthFilter extends AuthenticationFilter {
    @Override
    protected Properties getConfiguration(String configPrefix,
        FilterConfig filterConfig) throws ServletException {

      Properties properties =
          super.getConfiguration(configPrefix, filterConfig);

      properties.put(KerberosAuthenticationHandler.PRINCIPAL,
        httpSpnegoPrincipal);
      properties.put(KerberosAuthenticationHandler.KEYTAB,
        httpSpnegoKeytabFile.getAbsolutePath());
      properties.put(AuthenticationFilter.AUTH_TYPE, "simple");
      properties.put(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "false");
      return properties;
    }
  }

  private class SimpleAuth extends JerseyBinder {
    @Override
    public void configureScheduler() {
      isKerberosAuth = false;
      getConf().set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "simple");
    }
  }

  private class KerberosAuth extends JerseyBinder {
    @Override
    public void configureScheduler() {
      isKerberosAuth = true;
      getConf().set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
      getConf().set(YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY, httpSpnegoPrincipal);
      getConf().set(YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY,
          httpSpnegoKeytabFile.getAbsolutePath());
      getConf().set(YarnConfiguration.NM_WEBAPP_SPNEGO_USER_NAME_KEY,
          httpSpnegoPrincipal);
      getConf().set(YarnConfiguration.NM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY,
          httpSpnegoKeytabFile.getAbsolutePath());
    }
  }

  public static Collection<Object[]> guiceConfigs() {
    return Arrays.asList(new Object[][]{{0}, {1}});
  }

  public void initTestRMWebServicesDelegationTokens(int run) throws Exception {
    switch (run) {
    case 0:
    default:
      config.register(new KerberosAuth());
      break;
    case 1:
      config.register(new SimpleAuth());
      break;
    }
    setUp();
  }

  @BeforeAll
  public static void setupKDC() throws Exception {
    testRootDir = new File("target",
      TestRMWebServicesDelegationTokens.class.getName() + "-root");
    testMiniKDC = new MiniKdc(MiniKdc.createConf(), testRootDir);
    testMiniKDC.start();
    testMiniKDC.createPrincipal(httpSpnegoKeytabFile, "HTTP/localhost",
      "client", "client2", "client3");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    httpSpnegoKeytabFile.deleteOnExit();
    testRootDir.deleteOnExit();
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);
  }

  @AfterAll
  public static void shutdownKdc() {
    if (testMiniKDC != null) {
      testMiniKDC.stop();
    }
  }

  @AfterEach
  @Override
  public void tearDown() throws Exception {
    if (rm != null) {
      rm.stop();
    }
    super.tearDown();
    UserGroupInformation.setConfiguration(new Configuration());
  }

  // Simple test - try to create a delegation token via web services and check
  // to make sure we get back a valid token. Validate token using RM function
  // calls. It should only succeed with the kerberos filter
  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testCreateDelegationToken(int run) throws Exception {
    initTestRMWebServicesDelegationTokens(run);
    rm.start();
    final String renewer = "test-renewer";
    DelegationToken token = new DelegationToken();
    token.setRenewer(renewer);
    String jsonBody = toJson(token, DelegationToken.class);
    String xmlBody =
        "<delegation-token><renewer>" + renewer
            + "</renewer></delegation-token>";
    String[] mediaTypes =
        {MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML};
    Map<String, String> bodyMap = new HashMap<>();
    bodyMap.put(MediaType.APPLICATION_JSON, jsonBody);
    bodyMap.put(MediaType.APPLICATION_XML, xmlBody);
    for (final String mediaType : mediaTypes) {
      final String body = bodyMap.get(mediaType);
      for (final String contentType : mediaTypes) {
        if (isKerberosAuth) {
          when(request.getAuthType()).thenReturn("Kerberos");
          verifyKerberosAuthCreate(mediaType, contentType, body, renewer);
        } else {
          verifySimpleAuthCreate(mediaType, contentType, body);
        }
      }
    }

    rm.stop();
    return;
  }

  private void verifySimpleAuthCreate(String mediaType, String contentType,
      String body) {
    Response response =
        target().path("ws").path("v1").path("cluster")
          .path("delegation-token").queryParam("user.name", "testuser")
          .request(contentType)
          .post(Entity.entity(body, MediaType.valueOf(mediaType)), Response.class);
    assertResponseStatusCode(Response.Status.FORBIDDEN, response.getStatusInfo());
  }

  private void verifyKerberosAuthCreate(String mType, String cType,
      String reqBody, String renUser) throws Exception {
    final String mediaType = mType;
    final String contentType = cType;
    final String body = reqBody;
    final String renewer = renUser;
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Principal principal1 = () -> "client@EXAMPLE.COM";
        when(request.getUserPrincipal()).thenReturn(principal1);
        Response response =
            targetWithJsonObject().path("ws").path("v1").path("cluster")
            .path("delegation-token").request(contentType)
            .post(Entity.entity(body, MediaType.valueOf(mediaType)), Response.class);
        assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
        DelegationToken tok = getDelegationTokenFromResponse(response);
        assertFalse(tok.getToken().isEmpty());
        Token<RMDelegationTokenIdentifier> token =
            new Token<RMDelegationTokenIdentifier>();
        token.decodeFromUrlString(tok.getToken());
        assertEquals(renewer, token.decodeIdentifier().getRenewer().toString());
        assertValidRMToken(tok.getToken());
        DelegationToken dtoken = new DelegationToken();
        response =
            targetWithJsonObject().path("ws").path("v1").path("cluster")
            .path("delegation-token").request(contentType)
            .post(Entity.entity(toEntity(dtoken,
            DelegationToken.class, mediaType), mediaType), Response.class);
        assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
        tok = getDelegationTokenFromResponse(response);
        assertFalse(tok.getToken().isEmpty());
        token = new Token<RMDelegationTokenIdentifier>();
        token.decodeFromUrlString(tok.getToken());
        assertEquals("", token.decodeIdentifier().getRenewer().toString());
        assertValidRMToken(tok.getToken());
        return null;
      }
    });
  }

  // Test to verify renew functionality - create a token and then try to renew
  // it. The renewer should succeed; owner and third user should fail
  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testRenewDelegationToken(int run) throws Exception {
    initTestRMWebServicesDelegationTokens(run);
    this.client().register(new LoggingFeature());
    rm.start();
    final String renewer = "client2";
    final DelegationToken dummyToken = new DelegationToken();
    dummyToken.setRenewer(renewer);
    String[] mediaTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    for (final String mediaType : mediaTypes) {
      for (final String contentType : mediaTypes) {

        if (!isKerberosAuth) {
          verifySimpleAuthRenew(mediaType, contentType);
          continue;
        }

        if(isKerberosAuth) {
          when(request.getAuthType()).thenReturn("Kerberos");
        }

        // test "client" and client2" trying to renew "client" token
        final DelegationToken responseToken =
            KerberosTestUtils.doAsClient(new Callable<DelegationToken>() {
              @Override
              public DelegationToken call() throws Exception {
                Principal principal1 = () -> "client@EXAMPLE.COM";
                when(request.getUserPrincipal()).thenReturn(principal1);
                Response response =
                    targetWithJsonObject().path("ws").path("v1").path("cluster")
                    .path("delegation-token").request(contentType)
                    .post(Entity.entity(toEntity(dummyToken,
                    DelegationToken.class, mediaType), mediaType), Response.class);
                assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
                DelegationToken tok = getDelegationTokenFromResponse(response);
                assertFalse(tok.getToken().isEmpty());
                String body = generateRenewTokenBody(mediaType, tok.getToken());
                when(request.getHeader(yarnTokenHeader)).thenReturn(tok.getToken());
                response =
                    target().path("ws").path("v1").path("cluster")
                      .path("delegation-token").path("expiration").request(contentType)
                      .header(yarnTokenHeader, tok.getToken())
                      .post(Entity.entity(body, mediaType), Response.class);
                assertResponseStatusCode(Response.Status.FORBIDDEN,
                    response.getStatusInfo());
                return tok;
              }
            });

        KerberosTestUtils.doAs(renewer, new Callable<DelegationToken>() {
          @Override
          public DelegationToken call() throws Exception {
            Principal principal1 = () -> "client2@EXAMPLE.COM";
            when(request.getUserPrincipal()).thenReturn(principal1);
            when(request.getHeader(yarnTokenHeader)).thenReturn(responseToken.getToken());
            // renew twice so that we can confirm that the
            // expiration time actually changes
            long oldExpirationTime = Time.now();
            assertValidRMToken(responseToken.getToken());
            String body =
                generateRenewTokenBody(mediaType, responseToken.getToken());
            Response response =
                targetWithJsonObject().path("ws").path("v1").path("cluster")
                  .path("delegation-token").path("expiration")
                  .request(contentType).header(yarnTokenHeader, responseToken.getToken())
                  .post(Entity.entity(body, mediaType), Response.class);
            assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
            DelegationToken tok = getDelegationTokenFromResponse(response);
            String message =
                "Expiration time not as expected: old = " + oldExpirationTime
                    + "; new = " + tok.getNextExpirationTime();
            assertTrue(tok.getNextExpirationTime() > oldExpirationTime, message);
            oldExpirationTime = tok.getNextExpirationTime();
            // artificial sleep to ensure we get a different expiration time
            Thread.sleep(1000);
            response =
                  targetWithJsonObject().path("ws").path("v1").path("cluster")
                  .path("delegation-token").path("expiration")
                  .request(contentType).header(yarnTokenHeader, responseToken.getToken())
                  .post(Entity.entity(body, mediaType), Response.class);
            assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
            tok = getDelegationTokenFromResponse(response);
            message =
                "Expiration time not as expected: old = " + oldExpirationTime
                    + "; new = " + tok.getNextExpirationTime();
            assertTrue(tok.getNextExpirationTime() > oldExpirationTime, message);
            return tok;
          }
        });

        // test unauthorized user renew attempt
        KerberosTestUtils.doAs("client3", new Callable<DelegationToken>() {
          @Override
          public DelegationToken call() throws Exception {
            Principal principal1 = () -> "client3@EXAMPLE.COM";
            when(request.getUserPrincipal()).thenReturn(principal1);
            String body =
                generateRenewTokenBody(mediaType, responseToken.getToken());
            Response response =
                target().path("ws").path("v1").path("cluster")
                  .path("delegation-token").path("expiration").request(contentType)
                  .header(yarnTokenHeader, responseToken.getToken())
                  .post(Entity.entity(body, mediaType), Response.class);
            assertResponseStatusCode(Response.Status.FORBIDDEN,
                response.getStatusInfo());
            return null;
          }
        });

        // test bad request - incorrect format, empty token string and random
        // token string
        KerberosTestUtils.doAsClient(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            String token = "TEST_TOKEN_STRING";
            String body = "";
            if (mediaType.equals(MediaType.APPLICATION_JSON)) {
              DelegationToken dToken = new DelegationToken();
              dToken.setToken(token);
              body = toJson(dToken, DelegationToken.class);
            } else {
              body =
                  "<delegation-token><token>" + token
                      + "</token></delegation-token>";
            }

            // missing token header
            when(request.getHeader(yarnTokenHeader)).thenReturn(null);
            Response response =
                target().path("ws").path("v1").path("cluster")
                  .path("delegation-token").path("expiration").request()
                  .post(Entity.entity(body, MediaType.valueOf(mediaType)), Response.class);
            assertResponseStatusCode(Response.Status.BAD_REQUEST,
                response.getStatusInfo());
            return null;
          }
        });
      }
    }

    rm.stop();
  }

  private void verifySimpleAuthRenew(String mediaType, String contentType) throws Exception {
    String token = "TEST_TOKEN_STRING";
    String body = "";
    // contents of body don't matter because the request processing shouldn't
    // get that far
    if (mediaType.equals(MediaType.APPLICATION_JSON)) {
      DelegationToken delegationToken = new DelegationToken();
      body = "{\"token\": \"" + token + "\" }";
      delegationToken.setToken("test-123");
      body = toJson(delegationToken, DelegationToken.class);
    } else {
      body =
          "<delegation-token><token>" + token + "</token></delegation-token>";
      body = "<delegation-token><xml>abcd</xml></delegation-token>";
    }
    Response response =
        target().path("ws").path("v1").path("cluster")
          .path("delegation-token").queryParam("user.name", "testuser")
          .request(contentType)
          .post(Entity.entity(body, mediaType), Response.class);
    assertResponseStatusCode(Response.Status.FORBIDDEN, response.getStatusInfo());
  }

  // Test to verify cancel functionality - create a token and then try to cancel
  // it. The owner and renewer should succeed; third user should fail
  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testCancelDelegationToken(int run) throws Exception {
    initTestRMWebServicesDelegationTokens(run);
    rm.start();
    if (isKerberosAuth == false) {
      verifySimpleAuthCancel();
      return;
    }

    final DelegationToken dtoken = new DelegationToken();
    String renewer = "client2";
    dtoken.setRenewer(renewer);
    String[] mediaTypes =
        {MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML};
    when(request.getAuthType()).thenReturn("Kerberos");
    for (final String mediaType : mediaTypes) {
      for (final String contentType : mediaTypes) {

        // owner should be able to cancel delegation token
        KerberosTestUtils.doAsClient(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            Principal principal1 = () -> "client@EXAMPLE.COM";
            when(request.getUserPrincipal()).thenReturn(principal1);
            Response response =
                targetWithJsonObject().path("ws").path("v1").path("cluster")
                .path("delegation-token").request(contentType)
                .post(Entity.entity(toEntity(dtoken, DelegationToken.class, mediaType),
                mediaType), Response.class);
            assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
            DelegationToken tok = getDelegationTokenFromResponse(response);
            when(request.getHeader(yarnTokenHeader)).thenReturn(tok.getToken());
            response =
                targetWithJsonObject().path("ws").path("v1").path("cluster")
                .path("delegation-token").request(contentType)
                .header(yarnTokenHeader, tok.getToken())
                .delete(Response.class);
            assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
            assertTokenCancelled(tok.getToken());
            return null;
          }
        });

        // renewer should be able to cancel token
        final DelegationToken tmpToken =
            KerberosTestUtils.doAsClient(new Callable<DelegationToken>() {
              @Override
              public DelegationToken call() throws Exception {
                Principal principal1 = () -> "client@EXAMPLE.COM";
                when(request.getUserPrincipal()).thenReturn(principal1);
                Response response =
                    targetWithJsonObject().path("ws").path("v1").path("cluster").
                    path("delegation-token").request(contentType).
                    post(Entity.entity(toEntity(dtoken, DelegationToken.class, mediaType),
                    mediaType), Response.class);
                assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
                DelegationToken tok = getDelegationTokenFromResponse(response);
                return tok;
              }
            });

        KerberosTestUtils.doAs(renewer, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            Principal principal1 = () -> "client2@EXAMPLE.COM";
            when(request.getUserPrincipal()).thenReturn(principal1);
            when(request.getHeader(yarnTokenHeader)).thenReturn(tmpToken.getToken());
            Response response =
                    targetWithJsonObject().path("ws").path("v1").path("cluster")
                  .path("delegation-token").request()
                  .header(yarnTokenHeader, tmpToken.getToken())
                  .accept(contentType).delete(Response.class);
            assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
            assertTokenCancelled(tmpToken.getToken());
            return null;
          }
        });

        // third user should not be able to cancel token
        final DelegationToken tmpToken2 =
            KerberosTestUtils.doAsClient(new Callable<DelegationToken>() {
              @Override
              public DelegationToken call() throws Exception {
                Principal principal1 = () -> "client@EXAMPLE.COM";
                when(request.getUserPrincipal()).thenReturn(principal1);
                Response response =
                    targetWithJsonObject().path("ws").path("v1").path("cluster").
                    path("delegation-token").request(contentType).
                    post(Entity.entity(toEntity(dtoken, DelegationToken.class, mediaType),
                    mediaType), Response.class);
                assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
                DelegationToken tok = getDelegationTokenFromResponse(response);
                return tok;
              }
            });

        KerberosTestUtils.doAs("client3", new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            Principal principal1 = () -> "client3@EXAMPLE.COM";
            when(request.getUserPrincipal()).thenReturn(principal1);
            when(request.getHeader(yarnTokenHeader)).thenReturn(tmpToken2.getToken());
            Response response =
                targetWithJsonObject().path("ws").path("v1").path("cluster").
                path("delegation-token").request().
                header(yarnTokenHeader, tmpToken2.getToken()).
                accept(contentType).delete(Response.class);
            assertResponseStatusCode(Response.Status.FORBIDDEN,
                response.getStatusInfo());
            assertValidRMToken(tmpToken2.getToken());
            return null;
          }
        });

        testCancelTokenBadRequests(mediaType, contentType);
      }
    }

    rm.stop();
    return;
  }

  private void testCancelTokenBadRequests(String mType, String cType)
      throws Exception {

    final String mediaType = mType;
    final String contentType = cType;
    final DelegationToken dtoken = new DelegationToken();
    String renewer = "client2";
    dtoken.setRenewer(renewer);

    // bad request(invalid header value)
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Principal principal1 = () -> "client@EXAMPLE.COM";
        when(request.getUserPrincipal()).thenReturn(principal1);
        when(request.getHeader(yarnTokenHeader)).thenReturn("random-string");
        Response response =
            targetWithJsonObject().path("ws").path("v1").path("cluster")
            .path("delegation-token").request(contentType)
            .header(yarnTokenHeader, "random-string")
            .delete(Response.class);
        assertResponseStatusCode(Response.Status.BAD_REQUEST, response.getStatusInfo());
        return null;
      }
    });

    // bad request(missing header)
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Principal principal1 = () -> "client@EXAMPLE.COM";
        when(request.getUserPrincipal()).thenReturn(principal1);
        Response response =
            targetWithJsonObject().path("ws").path("v1").path("cluster")
            .path("delegation-token").request(contentType)
            .delete(Response.class);
        assertResponseStatusCode(Response.Status.BAD_REQUEST, response.getStatusInfo());
        return null;
      }
    });

    // bad request(cancelled token)
    final DelegationToken tmpToken =
        KerberosTestUtils.doAsClient(new Callable<DelegationToken>() {
          @Override
          public DelegationToken call() throws Exception {
            Principal principal1 = () -> "client@EXAMPLE.COM";
            when(request.getUserPrincipal()).thenReturn(principal1);
            Response response =
                targetWithJsonObject().path("ws").path("v1").path("cluster")
                .path("delegation-token").request(contentType)
                .post(Entity.entity(toEntity(dtoken, DelegationToken.class, mediaType),
                mediaType), Response.class);
            assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
            DelegationToken tok = getDelegationTokenFromResponse(response);
            return tok;
          }
        });

    KerberosTestUtils.doAs(renewer, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Principal principal1 = () -> "client2@EXAMPLE.COM";
        when(request.getHeader(yarnTokenHeader)).thenReturn(tmpToken.getToken());
        when(request.getUserPrincipal()).thenReturn(principal1);
        Response response =
                targetWithJsonObject().path("ws").path("v1").path("cluster")
              .path("delegation-token").request(contentType)
              .header(yarnTokenHeader, tmpToken.getToken())
              .delete(Response.class);
        assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
        response =
                targetWithJsonObject().path("ws").path("v1").path("cluster")
              .path("delegation-token").request(contentType)
              .header(yarnTokenHeader, tmpToken.getToken())
              .delete(Response.class);
        assertResponseStatusCode(Response.Status.BAD_REQUEST, response.getStatusInfo());
        return null;
      }
    });
  }

  private void verifySimpleAuthCancel() {
    // contents of header don't matter; request should never get that far
    Response response =
        target().path("ws").path("v1").path("cluster")
          .path("delegation-token").queryParam("user.name", "testuser")
          .request()
          .header(RMWebServices.DELEGATION_TOKEN_HEADER, "random")
          .delete(Response.class);
    assertResponseStatusCode(Response.Status.FORBIDDEN, response.getStatusInfo());
  }

  private DelegationToken
      getDelegationTokenFromResponse(Response response)
          throws IOException, ParserConfigurationException, SAXException,
          JSONException {
    if (response.getMediaType().toString().contains(MediaType.APPLICATION_JSON)) {
      return getDelegationTokenFromJson(
          response.readEntity(JSONObject.class).getJSONObject("delegation-token"));
    }
    return getDelegationTokenFromXML(response.readEntity(String.class));
  }

  public static DelegationToken getDelegationTokenFromXML(String tokenXML)
      throws IOException, ParserConfigurationException, SAXException {
    DocumentBuilderFactory dbf = XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(tokenXML));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("delegation-token");
    assertEquals(1, nodes.getLength(), "incorrect number of elements");
    Element element = (Element) nodes.item(0);
    DelegationToken ret = new DelegationToken();
    String token = WebServicesTestUtils.getXmlString(element, "token");
    if (token != null) {
      ret.setToken(token);
    } else {
      long expiration =
          WebServicesTestUtils.getXmlLong(element, "expiration-time");
      ret.setNextExpirationTime(expiration);
    }
    return ret;
  }

  public static DelegationToken getDelegationTokenFromJson(JSONObject json)
      throws JSONException {
    DelegationToken ret = new DelegationToken();
    if (json.has("token")) {
      ret.setToken(json.getString("token"));
    } else if (json.has("expiration-time")) {
      ret.setNextExpirationTime(json.getLong("expiration-time"));
    }
    return ret;
  }

  private void assertValidRMToken(String encodedToken) throws IOException {
    Token<RMDelegationTokenIdentifier> realToken =
        new Token<RMDelegationTokenIdentifier>();
    realToken.decodeFromUrlString(encodedToken);
    RMDelegationTokenIdentifier ident = rm.getRMContext()
      .getRMDelegationTokenSecretManager().decodeTokenIdentifier(realToken);
    rm.getRMContext().getRMDelegationTokenSecretManager()
      .verifyToken(ident, realToken.getPassword());
    assertTrue(rm.getRMContext().getRMDelegationTokenSecretManager()
      .getAllTokens().containsKey(ident));
  }

  private void assertTokenCancelled(String encodedToken) throws Exception {
    Token<RMDelegationTokenIdentifier> realToken =
        new Token<RMDelegationTokenIdentifier>();
    realToken.decodeFromUrlString(encodedToken);
    RMDelegationTokenIdentifier ident = rm.getRMContext()
      .getRMDelegationTokenSecretManager().decodeTokenIdentifier(realToken);
    boolean exceptionCaught = false;
    try {
      rm.getRMContext().getRMDelegationTokenSecretManager()
        .verifyToken(ident, realToken.getPassword());
    } catch (InvalidToken it) {
      exceptionCaught = true;
    }
    assertTrue(exceptionCaught, "InvalidToken exception not thrown");
    assertFalse(rm.getRMContext().getRMDelegationTokenSecretManager()
      .getAllTokens().containsKey(ident));
  }

  private static String generateRenewTokenBody(String mediaType, String token)
      throws Exception {
    String body = "";
    if (mediaType.contains(MediaType.APPLICATION_JSON)) {
      DelegationToken dToken = new DelegationToken();
      dToken.setToken(token);
      body = toJson(dToken, DelegationToken.class);
    } else {
      body =
          "<delegation-token><token>" + token + "</token></delegation-token>";
    }
    return body;
  }
}

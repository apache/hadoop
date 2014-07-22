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

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.DelegationToken;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.filter.LoggingFilter;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;

@RunWith(Parameterized.class)
public class TestRMWebServicesDelegationTokens extends JerseyTest {

  private static File testRootDir;
  private static File httpSpnegoKeytabFile = new File(
    KerberosTestUtils.getKeytabFile());
  private static String httpSpnegoPrincipal = KerberosTestUtils
    .getServerPrincipal();
  private static MiniKdc testMiniKDC;
  private static MockRM rm;
  private Injector injector;
  private boolean isKerberosAuth = false;

  // Make sure the test uses the published header string
  final String yarnTokenHeader = "Hadoop-YARN-RM-Delegation-Token";

  @Singleton
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

  @Singleton
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

  private class TestServletModule extends ServletModule {
    public Configuration rmconf = new Configuration();

    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      Configuration rmconf = new Configuration();
      rmconf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      rmconf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
        ResourceScheduler.class);
      rmconf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
      rm = new MockRM(rmconf);
      bind(ResourceManager.class).toInstance(rm);
      bind(RMContext.class).toInstance(rm.getRMContext());
      bind(ApplicationACLsManager.class).toInstance(
        rm.getApplicationACLsManager());
      bind(QueueACLsManager.class).toInstance(rm.getQueueACLsManager());
      if (isKerberosAuth == true) {
        filter("/*").through(TestKerberosAuthFilter.class);
      } else {
        filter("/*").through(TestSimpleAuthFilter.class);
      }
      serve("/*").with(GuiceContainer.class);
    }
  }

  private Injector getSimpleAuthInjector() {
    return Guice.createInjector(new TestServletModule() {
      @Override
      protected void configureServlets() {
        isKerberosAuth = false;
        rmconf.set(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
          "simple");
        super.configureServlets();
      }
    });
  }

  private Injector getKerberosAuthInjector() {
    return Guice.createInjector(new TestServletModule() {
      @Override
      protected void configureServlets() {
        isKerberosAuth = true;
        rmconf.set(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
          "kerberos");
        rmconf.set(YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY,
          httpSpnegoPrincipal);
        rmconf.set(YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY,
          httpSpnegoKeytabFile.getAbsolutePath());
        rmconf.set(YarnConfiguration.NM_WEBAPP_SPNEGO_USER_NAME_KEY,
          httpSpnegoPrincipal);
        rmconf.set(YarnConfiguration.NM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY,
          httpSpnegoKeytabFile.getAbsolutePath());

        super.configureServlets();
      }
    });
  }

  public class GuiceServletConfig extends GuiceServletContextListener {

    @Override
    protected Injector getInjector() {
      return injector;
    }
  }

  @Parameters
  public static Collection<Object[]> guiceConfigs() {
    return Arrays.asList(new Object[][] { { 0 }, { 1 } });
  }

  public TestRMWebServicesDelegationTokens(int run) throws Exception {
    super(new WebAppDescriptor.Builder(
      "org.apache.hadoop.yarn.server.resourcemanager.webapp")
      .contextListenerClass(GuiceServletConfig.class)
      .filterClass(com.google.inject.servlet.GuiceFilter.class)
      .contextPath("jersey-guice-filter").servletPath("/").build());
    switch (run) {
    case 0:
    default:
      injector = getKerberosAuthInjector();
      break;
    case 1:
      injector = getSimpleAuthInjector();
      break;
    }
  }

  @BeforeClass
  public static void setupKDC() throws Exception {
    testRootDir = new File("target",
      TestRMWebServicesDelegationTokens.class.getName() + "-root");
    testMiniKDC = new MiniKdc(MiniKdc.createConf(), testRootDir);
    testMiniKDC.start();
    testMiniKDC.createPrincipal(httpSpnegoKeytabFile, "HTTP/localhost",
      "client", "client2", "client3");
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    httpSpnegoKeytabFile.deleteOnExit();
    testRootDir.deleteOnExit();
  }

  @AfterClass
  public static void shutdownKdc() {
    if (testMiniKDC != null) {
      testMiniKDC.stop();
    }
  }

  @After
  @Override
  public void tearDown() throws Exception {
    rm.stop();
    super.tearDown();
  }

  // Simple test - try to create a delegation token via web services and check
  // to make sure we get back a valid token. Validate token using RM function
  // calls. It should only succeed with the kerberos filter
  @Test
  public void testCreateDelegationToken() throws Exception {
    rm.start();
    this.client().addFilter(new LoggingFilter(System.out));
    final String renewer = "test-renewer";
    String jsonBody = "{ \"renewer\" : \"" + renewer + "\" }";
    String xmlBody =
        "<delegation-token><renewer>" + renewer
            + "</renewer></delegation-token>";
    String[] mediaTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    Map<String, String> bodyMap = new HashMap<String, String>();
    bodyMap.put(MediaType.APPLICATION_JSON, jsonBody);
    bodyMap.put(MediaType.APPLICATION_XML, xmlBody);
    for (final String mediaType : mediaTypes) {
      final String body = bodyMap.get(mediaType);
      for (final String contentType : mediaTypes) {
        if (isKerberosAuth == true) {
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
    ClientResponse response =
        resource().path("ws").path("v1").path("cluster")
          .path("delegation-token").queryParam("user.name", "testuser")
          .accept(contentType).entity(body, mediaType)
          .post(ClientResponse.class);
    assertEquals(Status.FORBIDDEN, response.getClientResponseStatus());
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
        ClientResponse response =
            resource().path("ws").path("v1").path("cluster")
              .path("delegation-token").accept(contentType)
              .entity(body, mediaType).post(ClientResponse.class);
        assertEquals(Status.OK, response.getClientResponseStatus());
        DelegationToken tok = getDelegationTokenFromResponse(response);
        assertFalse(tok.getToken().isEmpty());
        Token<RMDelegationTokenIdentifier> token =
            new Token<RMDelegationTokenIdentifier>();
        token.decodeFromUrlString(tok.getToken());
        assertEquals(renewer, token.decodeIdentifier().getRenewer().toString());
        assertValidRMToken(tok.getToken());
        DelegationToken dtoken = new DelegationToken();
        response =
            resource().path("ws").path("v1").path("cluster")
              .path("delegation-token").accept(contentType)
              .entity(dtoken, mediaType).post(ClientResponse.class);
        assertEquals(Status.OK, response.getClientResponseStatus());
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
  @Test
  public void testRenewDelegationToken() throws Exception {
    client().addFilter(new LoggingFilter(System.out));
    rm.start();
    final String renewer = "client2";
    this.client().addFilter(new LoggingFilter(System.out));
    final DelegationToken dummyToken = new DelegationToken();
    dummyToken.setRenewer(renewer);
    String[] mediaTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    for (final String mediaType : mediaTypes) {
      for (final String contentType : mediaTypes) {

        if (isKerberosAuth == false) {
          verifySimpleAuthRenew(mediaType, contentType);
          continue;
        }

        // test "client" and client2" trying to renew "client" token
        final DelegationToken responseToken =
            KerberosTestUtils.doAsClient(new Callable<DelegationToken>() {
              @Override
              public DelegationToken call() throws Exception {
                ClientResponse response =
                    resource().path("ws").path("v1").path("cluster")
                      .path("delegation-token").accept(contentType)
                      .entity(dummyToken, mediaType).post(ClientResponse.class);
                assertEquals(Status.OK, response.getClientResponseStatus());
                DelegationToken tok = getDelegationTokenFromResponse(response);
                assertFalse(tok.getToken().isEmpty());
                String body = generateRenewTokenBody(mediaType, tok.getToken());
                response =
                    resource().path("ws").path("v1").path("cluster")
                      .path("delegation-token").path("expiration")
                      .header(yarnTokenHeader, tok.getToken())
                      .accept(contentType).entity(body, mediaType)
                      .post(ClientResponse.class);
                assertEquals(Status.FORBIDDEN,
                  response.getClientResponseStatus());
                return tok;
              }
            });

        KerberosTestUtils.doAs(renewer, new Callable<DelegationToken>() {
          @Override
          public DelegationToken call() throws Exception {
            // renew twice so that we can confirm that the
            // expiration time actually changes
            long oldExpirationTime = Time.now();
            assertValidRMToken(responseToken.getToken());
            String body =
                generateRenewTokenBody(mediaType, responseToken.getToken());
            ClientResponse response =
                resource().path("ws").path("v1").path("cluster")
                  .path("delegation-token").path("expiration")
                  .header(yarnTokenHeader, responseToken.getToken())
                  .accept(contentType).entity(body, mediaType)
                  .post(ClientResponse.class);
            assertEquals(Status.OK, response.getClientResponseStatus());
            DelegationToken tok = getDelegationTokenFromResponse(response);
            String message =
                "Expiration time not as expected: old = " + oldExpirationTime
                    + "; new = " + tok.getNextExpirationTime();
            assertTrue(message, tok.getNextExpirationTime() > oldExpirationTime);
            oldExpirationTime = tok.getNextExpirationTime();
            // artificial sleep to ensure we get a different expiration time
            Thread.sleep(1000);
            response =
                resource().path("ws").path("v1").path("cluster")
                  .path("delegation-token").path("expiration")
                  .header(yarnTokenHeader, responseToken.getToken())
                  .accept(contentType).entity(body, mediaType)
                  .post(ClientResponse.class);
            assertEquals(Status.OK, response.getClientResponseStatus());
            tok = getDelegationTokenFromResponse(response);
            message =
                "Expiration time not as expected: old = " + oldExpirationTime
                    + "; new = " + tok.getNextExpirationTime();
            assertTrue(message, tok.getNextExpirationTime() > oldExpirationTime);
            return tok;
          }
        });

        // test unauthorized user renew attempt
        KerberosTestUtils.doAs("client3", new Callable<DelegationToken>() {
          @Override
          public DelegationToken call() throws Exception {
            String body =
                generateRenewTokenBody(mediaType, responseToken.getToken());
            ClientResponse response =
                resource().path("ws").path("v1").path("cluster")
                  .path("delegation-token").path("expiration")
                  .header(yarnTokenHeader, responseToken.getToken())
                  .accept(contentType).entity(body, mediaType)
                  .post(ClientResponse.class);
            assertEquals(Status.FORBIDDEN, response.getClientResponseStatus());
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
              body = "{\"token\": \"" + token + "\" }";
            } else {
              body =
                  "<delegation-token><token>" + token
                      + "</token></delegation-token>";
            }

            // missing token header
            ClientResponse response =
                resource().path("ws").path("v1").path("cluster")
                  .path("delegation-token").path("expiration")
                  .accept(contentType).entity(body, mediaType)
                  .post(ClientResponse.class);
            assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
            return null;
          }
        });
      }
    }

    rm.stop();
    return;
  }

  private void verifySimpleAuthRenew(String mediaType, String contentType) {
    String token = "TEST_TOKEN_STRING";
    String body = "";
    // contents of body don't matter because the request processing shouldn't
    // get that far
    if (mediaType.equals(MediaType.APPLICATION_JSON)) {
      body = "{\"token\": \"" + token + "\" }";
      body = "{\"abcd\": \"test-123\" }";
    } else {
      body =
          "<delegation-token><token>" + token + "</token></delegation-token>";
      body = "<delegation-token><xml>abcd</xml></delegation-token>";
    }
    ClientResponse response =
        resource().path("ws").path("v1").path("cluster")
          .path("delegation-token").queryParam("user.name", "testuser")
          .accept(contentType).entity(body, mediaType)
          .post(ClientResponse.class);
    assertEquals(Status.FORBIDDEN, response.getClientResponseStatus());
  }

  // Test to verify cancel functionality - create a token and then try to cancel
  // it. The owner and renewer should succeed; third user should fail
  @Test
  public void testCancelDelegationToken() throws Exception {
    rm.start();
    this.client().addFilter(new LoggingFilter(System.out));
    if (isKerberosAuth == false) {
      verifySimpleAuthCancel();
      return;
    }

    final DelegationToken dtoken = new DelegationToken();
    String renewer = "client2";
    dtoken.setRenewer(renewer);
    String[] mediaTypes =
        { MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML };
    for (final String mediaType : mediaTypes) {
      for (final String contentType : mediaTypes) {

        // owner should be able to cancel delegation token
        KerberosTestUtils.doAsClient(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            ClientResponse response =
                resource().path("ws").path("v1").path("cluster")
                  .path("delegation-token").accept(contentType)
                  .entity(dtoken, mediaType).post(ClientResponse.class);
            assertEquals(Status.OK, response.getClientResponseStatus());
            DelegationToken tok = getDelegationTokenFromResponse(response);
            response =
                resource().path("ws").path("v1").path("cluster")
                  .path("delegation-token")
                  .header(yarnTokenHeader, tok.getToken()).accept(contentType)
                  .delete(ClientResponse.class);
            assertEquals(Status.OK, response.getClientResponseStatus());
            assertTokenCancelled(tok.getToken());
            return null;
          }
        });

        // renewer should be able to cancel token
        final DelegationToken tmpToken =
            KerberosTestUtils.doAsClient(new Callable<DelegationToken>() {
              @Override
              public DelegationToken call() throws Exception {
                ClientResponse response =
                    resource().path("ws").path("v1").path("cluster")
                      .path("delegation-token").accept(contentType)
                      .entity(dtoken, mediaType).post(ClientResponse.class);
                assertEquals(Status.OK, response.getClientResponseStatus());
                DelegationToken tok = getDelegationTokenFromResponse(response);
                return tok;
              }
            });

        KerberosTestUtils.doAs(renewer, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            ClientResponse response =
                resource().path("ws").path("v1").path("cluster")
                  .path("delegation-token")
                  .header(yarnTokenHeader, tmpToken.getToken())
                  .accept(contentType).delete(ClientResponse.class);
            assertEquals(Status.OK, response.getClientResponseStatus());
            assertTokenCancelled(tmpToken.getToken());
            return null;
          }
        });

        // third user should not be able to cancel token
        final DelegationToken tmpToken2 =
            KerberosTestUtils.doAsClient(new Callable<DelegationToken>() {
              @Override
              public DelegationToken call() throws Exception {
                ClientResponse response =
                    resource().path("ws").path("v1").path("cluster")
                      .path("delegation-token").accept(contentType)
                      .entity(dtoken, mediaType).post(ClientResponse.class);
                assertEquals(Status.OK, response.getClientResponseStatus());
                DelegationToken tok = getDelegationTokenFromResponse(response);
                return tok;
              }
            });

        KerberosTestUtils.doAs("client3", new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            ClientResponse response =
                resource().path("ws").path("v1").path("cluster")
                  .path("delegation-token")
                  .header(yarnTokenHeader, tmpToken2.getToken())
                  .accept(contentType).delete(ClientResponse.class);
            assertEquals(Status.FORBIDDEN, response.getClientResponseStatus());
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
        ClientResponse response =
            resource().path("ws").path("v1").path("cluster")
              .path("delegation-token")
              .header(yarnTokenHeader, "random-string").accept(contentType)
              .delete(ClientResponse.class);
        assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
        return null;
      }
    });

    // bad request(missing header)
    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        ClientResponse response =
            resource().path("ws").path("v1").path("cluster")
              .path("delegation-token").accept(contentType)
              .delete(ClientResponse.class);
        assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());

        return null;
      }
    });

    // bad request(cancelled token)
    final DelegationToken tmpToken =
        KerberosTestUtils.doAsClient(new Callable<DelegationToken>() {
          @Override
          public DelegationToken call() throws Exception {
            ClientResponse response =
                resource().path("ws").path("v1").path("cluster")
                  .path("delegation-token").accept(contentType)
                  .entity(dtoken, mediaType).post(ClientResponse.class);
            assertEquals(Status.OK, response.getClientResponseStatus());
            DelegationToken tok = getDelegationTokenFromResponse(response);
            return tok;
          }
        });

    KerberosTestUtils.doAs(renewer, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        ClientResponse response =
            resource().path("ws").path("v1").path("cluster")
              .path("delegation-token")
              .header(yarnTokenHeader, tmpToken.getToken()).accept(contentType)
              .delete(ClientResponse.class);
        assertEquals(Status.OK, response.getClientResponseStatus());
        response =
            resource().path("ws").path("v1").path("cluster")
              .path("delegation-token")
              .header(yarnTokenHeader, tmpToken.getToken()).accept(contentType)
              .delete(ClientResponse.class);
        assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
        return null;
      }
    });
  }

  private void verifySimpleAuthCancel() {
    // contents of header don't matter; request should never get that far
    ClientResponse response =
        resource().path("ws").path("v1").path("cluster")
          .path("delegation-token").queryParam("user.name", "testuser")
          .header(RMWebServices.DELEGATION_TOKEN_HEADER, "random")
          .delete(ClientResponse.class);
    assertEquals(Status.FORBIDDEN, response.getClientResponseStatus());
  }

  private DelegationToken
      getDelegationTokenFromResponse(ClientResponse response)
          throws IOException, ParserConfigurationException, SAXException,
          JSONException {
    if (response.getType().toString().equals(MediaType.APPLICATION_JSON)) {
      return getDelegationTokenFromJson(response.getEntity(JSONObject.class));
    }
    return getDelegationTokenFromXML(response.getEntity(String.class));
  }

  public static DelegationToken getDelegationTokenFromXML(String tokenXML)
      throws IOException, ParserConfigurationException, SAXException {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(tokenXML));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("delegation-token");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
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
    RMDelegationTokenIdentifier ident = realToken.decodeIdentifier();
    rm.getRMContext().getRMDelegationTokenSecretManager()
      .verifyToken(ident, realToken.getPassword());
    assertTrue(rm.getRMContext().getRMDelegationTokenSecretManager()
      .getAllTokens().containsKey(ident));
  }

  private void assertTokenCancelled(String encodedToken) throws Exception {
    Token<RMDelegationTokenIdentifier> realToken =
        new Token<RMDelegationTokenIdentifier>();
    realToken.decodeFromUrlString(encodedToken);
    RMDelegationTokenIdentifier ident = realToken.decodeIdentifier();
    boolean exceptionCaught = false;
    try {
      rm.getRMContext().getRMDelegationTokenSecretManager()
        .verifyToken(ident, realToken.getPassword());
    } catch (InvalidToken it) {
      exceptionCaught = true;
    }
    assertTrue("InvalidToken exception not thrown", exceptionCaught);
    assertFalse(rm.getRMContext().getRMDelegationTokenSecretManager()
      .getAllTokens().containsKey(ident));
  }

  private static String generateRenewTokenBody(String mediaType, String token) {
    String body = "";
    if (mediaType.equals(MediaType.APPLICATION_JSON)) {
      body = "{\"token\": \"" + token + "\" }";
    } else {
      body =
          "<delegation-token><token>" + token + "</token></delegation-token>";
    }
    return body;
  }
}

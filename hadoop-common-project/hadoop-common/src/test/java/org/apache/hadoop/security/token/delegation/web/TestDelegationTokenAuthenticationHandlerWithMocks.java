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
package org.apache.hadoop.security.token.delegation.web;

import static org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator.DelegationTokenOperation.*;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Properties;

public class TestDelegationTokenAuthenticationHandlerWithMocks {

  public static class MockDelegationTokenAuthenticationHandler
      extends DelegationTokenAuthenticationHandler {

    public MockDelegationTokenAuthenticationHandler() {
      super(new AuthenticationHandler() {
        @Override
        public String getType() {
          return "T";
        }

        @Override
        public void init(Properties config) throws ServletException {

        }

        @Override
        public void destroy() {

        }

        @Override
        public boolean managementOperation(AuthenticationToken token,
            HttpServletRequest request, HttpServletResponse response)
            throws IOException, AuthenticationException {
          return false;
        }

        @Override
        public AuthenticationToken authenticate(HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, AuthenticationException {
          response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
          response.setHeader(KerberosAuthenticator.WWW_AUTHENTICATE, "mock");
          return null;
        }
      });
    }

  }

  private DelegationTokenAuthenticationHandler handler;

  @Rule
  public Timeout testTimeout = new Timeout(120000);

  @Before
  public void setUp() throws Exception {
    Properties conf = new Properties();

    conf.put(KerberosDelegationTokenAuthenticationHandler.TOKEN_KIND, "foo");
    handler = new MockDelegationTokenAuthenticationHandler();
    handler.initTokenManager(conf);
  }

  @After
  public void cleanUp() {
      handler.destroy();
  }

  @Test
  public void testManagementOperations() throws Exception {
    final Text testTokenKind = new Text("foo");
    final String testRenewer = "bar";
    final String testService = "192.168.64.101:8888";
    testNonManagementOperation();
    testManagementOperationErrors();
    testGetToken(null, null, testTokenKind);
    testGetToken(testRenewer, null, testTokenKind);
    testCancelToken();
    testRenewToken(testRenewer);

    // Management operations against token requested with service parameter
    Token<DelegationTokenIdentifier> testToken =
        testGetToken(testRenewer, testService, testTokenKind);
    testRenewToken(testToken, testRenewer);
    testCancelToken(testToken);
  }

  private void testNonManagementOperation() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getParameter(
        DelegationTokenAuthenticator.OP_PARAM)).thenReturn(null);
    Assert.assertTrue(handler.managementOperation(null, request, null));
    Mockito.when(request.getParameter(
        DelegationTokenAuthenticator.OP_PARAM)).thenReturn("CREATE");
    Assert.assertTrue(handler.managementOperation(null, request, null));
  }

  private void testManagementOperationErrors() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getQueryString()).thenReturn(
        DelegationTokenAuthenticator.OP_PARAM + "=" +
            DelegationTokenAuthenticator.DelegationTokenOperation.
                GETDELEGATIONTOKEN.toString()
    );
    Mockito.when(request.getMethod()).thenReturn("FOO");
    Assert.assertFalse(handler.managementOperation(null, request, response));
    Mockito.verify(response).sendError(
        Mockito.eq(HttpServletResponse.SC_BAD_REQUEST),
        Mockito.startsWith("Wrong HTTP method"));

    Mockito.reset(response);
    Mockito.when(request.getMethod()).thenReturn(
        DelegationTokenAuthenticator.DelegationTokenOperation.
            GETDELEGATIONTOKEN.getHttpMethod()
    );
    Assert.assertFalse(handler.managementOperation(null, request, response));
    Mockito.verify(response).setStatus(
        Mockito.eq(HttpServletResponse.SC_UNAUTHORIZED));
    Mockito.verify(response).setHeader(
        Mockito.eq(KerberosAuthenticator.WWW_AUTHENTICATE),
        Mockito.eq("mock"));
  }

  private Token<DelegationTokenIdentifier> testGetToken(String renewer,
      String service, Text expectedTokenKind) throws Exception {
    DelegationTokenAuthenticator.DelegationTokenOperation op =
        DelegationTokenAuthenticator.DelegationTokenOperation.
            GETDELEGATIONTOKEN;
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getQueryString()).
        thenReturn(DelegationTokenAuthenticator.OP_PARAM + "=" + op.toString());
    Mockito.when(request.getMethod()).thenReturn(op.getHttpMethod());

    AuthenticationToken token = Mockito.mock(AuthenticationToken.class);
    Mockito.when(token.getUserName()).thenReturn("user");
    Mockito.when(response.getWriter()).thenReturn(new PrintWriter(
        new StringWriter()));
    Assert.assertFalse(handler.managementOperation(token, request, response));

    String queryString =
        DelegationTokenAuthenticator.OP_PARAM + "=" + op.toString() + "&" +
        DelegationTokenAuthenticator.RENEWER_PARAM + "=" + renewer;
    if (service != null) {
      queryString += "&" + DelegationTokenAuthenticator.SERVICE_PARAM + "="
          + service;
    }
    Mockito.when(request.getQueryString()).thenReturn(queryString);
    Mockito.reset(response);
    Mockito.reset(token);
    Mockito.when(token.getUserName()).thenReturn("user");
    StringWriter writer = new StringWriter();
    PrintWriter pwriter = new PrintWriter(writer);
    Mockito.when(response.getWriter()).thenReturn(pwriter);
    Assert.assertFalse(handler.managementOperation(token, request, response));
    if (renewer == null) {
      Mockito.verify(token).getUserName();
    } else {
      Mockito.verify(token).getUserName();
    }
    Mockito.verify(response).setStatus(HttpServletResponse.SC_OK);
    Mockito.verify(response).setContentType(MediaType.APPLICATION_JSON);
    pwriter.close();
    String responseOutput = writer.toString();
    String tokenLabel = DelegationTokenAuthenticator.
        DELEGATION_TOKEN_JSON;
    Assert.assertTrue(responseOutput.contains(tokenLabel));
    Assert.assertTrue(responseOutput.contains(
        DelegationTokenAuthenticator.DELEGATION_TOKEN_URL_STRING_JSON));
    ObjectMapper jsonMapper = new ObjectMapper();
    Map json = jsonMapper.readValue(responseOutput, Map.class);
    json = (Map) json.get(tokenLabel);
    String tokenStr;
    tokenStr = (String) json.get(DelegationTokenAuthenticator.
        DELEGATION_TOKEN_URL_STRING_JSON);
    Token<DelegationTokenIdentifier> dt = new Token<DelegationTokenIdentifier>();
    dt.decodeFromUrlString(tokenStr);
    handler.getTokenManager().verifyToken(dt);
    Assert.assertEquals(expectedTokenKind, dt.getKind());
    if (service != null) {
      Assert.assertEquals(service, dt.getService().toString());
    } else {
      Assert.assertEquals(0, dt.getService().getLength());
    }
    return dt;
  }

  @SuppressWarnings("unchecked")
  private void testCancelToken() throws Exception {
    Token<DelegationTokenIdentifier> token =
        (Token<DelegationTokenIdentifier>) handler.getTokenManager()
            .createToken(UserGroupInformation.getCurrentUser(), "foo");
    testCancelToken(token);
  }

  @SuppressWarnings("unchecked")
  private void testCancelToken(Token<DelegationTokenIdentifier> token)
      throws Exception {
    DelegationTokenAuthenticator.DelegationTokenOperation op =
        DelegationTokenAuthenticator.DelegationTokenOperation.
            CANCELDELEGATIONTOKEN;
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getQueryString()).thenReturn(
        DelegationTokenAuthenticator.OP_PARAM + "=" + op.toString());
    Mockito.when(request.getMethod()).
        thenReturn(op.getHttpMethod());

    Assert.assertFalse(handler.managementOperation(null, request, response));
    Mockito.verify(response).sendError(
        Mockito.eq(HttpServletResponse.SC_BAD_REQUEST),
        Mockito.contains("requires the parameter [token]"));

    Mockito.reset(response);
    Mockito.when(request.getQueryString()).thenReturn(
        DelegationTokenAuthenticator.OP_PARAM + "=" + op.toString() + "&" +
            DelegationTokenAuthenticator.TOKEN_PARAM + "=" +
            token.encodeToUrlString()
    );
    Assert.assertFalse(handler.managementOperation(null, request, response));
    Mockito.verify(response).setStatus(HttpServletResponse.SC_OK);
    try {
      handler.getTokenManager().verifyToken(token);
      Assert.fail();
    } catch (SecretManager.InvalidToken ex) {
      //NOP
    } catch (Throwable ex) {
      Assert.fail();
    }
  }

  @SuppressWarnings("unchecked")
  private void testRenewToken(String testRenewer) throws Exception {
    Token<DelegationTokenIdentifier> dToken = (Token<DelegationTokenIdentifier>)
        handler.getTokenManager().createToken(
            UserGroupInformation.getCurrentUser(), testRenewer);
    testRenewToken(dToken, testRenewer);
  }

  @SuppressWarnings("unchecked")
  private void testRenewToken(Token<DelegationTokenIdentifier> dToken,
      String testRenewer) throws Exception {
    DelegationTokenAuthenticator.DelegationTokenOperation op =
        DelegationTokenAuthenticator.DelegationTokenOperation.
            RENEWDELEGATIONTOKEN;
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getQueryString()).
        thenReturn(DelegationTokenAuthenticator.OP_PARAM + "=" + op.toString());
    Mockito.when(request.getMethod()).
        thenReturn(op.getHttpMethod());

    Assert.assertFalse(handler.managementOperation(null, request, response));
    Mockito.verify(response).setStatus(
        Mockito.eq(HttpServletResponse.SC_UNAUTHORIZED));
    Mockito.verify(response).setHeader(Mockito.eq(
            KerberosAuthenticator.WWW_AUTHENTICATE),
        Mockito.eq("mock")
    );

    Mockito.reset(response);
    AuthenticationToken token = Mockito.mock(AuthenticationToken.class);
    Mockito.when(token.getUserName()).thenReturn(testRenewer);
    Assert.assertFalse(handler.managementOperation(token, request, response));
    Mockito.verify(response).sendError(
        Mockito.eq(HttpServletResponse.SC_BAD_REQUEST),
        Mockito.contains("requires the parameter [token]"));

    Mockito.reset(response);
    StringWriter writer = new StringWriter();
    PrintWriter pwriter = new PrintWriter(writer);
    Mockito.when(response.getWriter()).thenReturn(pwriter);

    Mockito.when(request.getQueryString()).
        thenReturn(DelegationTokenAuthenticator.OP_PARAM + "=" + op.toString() +
            "&" + DelegationTokenAuthenticator.TOKEN_PARAM + "=" +
            dToken.encodeToUrlString());
    Assert.assertFalse(handler.managementOperation(token, request, response));
    Mockito.verify(response).setStatus(HttpServletResponse.SC_OK);
    pwriter.close();
    Assert.assertTrue(writer.toString().contains("long"));
    handler.getTokenManager().verifyToken(dToken);
  }

  @Test
  public void testAuthenticate() throws Exception {
    testValidDelegationTokenQueryString();
    testValidDelegationTokenHeader();
    testInvalidDelegationTokenQueryString();
    testInvalidDelegationTokenHeader();
  }

  @SuppressWarnings("unchecked")
  private void testValidDelegationTokenQueryString() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Token<DelegationTokenIdentifier> dToken =
        (Token<DelegationTokenIdentifier>) handler.getTokenManager().createToken(
            UserGroupInformation.getCurrentUser(), "user");
    Mockito.when(request.getQueryString()).thenReturn(
        DelegationTokenAuthenticator.DELEGATION_PARAM + "=" +
        dToken.encodeToUrlString());

    AuthenticationToken token = handler.authenticate(request, response);
    Assert.assertEquals(UserGroupInformation.getCurrentUser().
            getShortUserName(), token.getUserName());
    Assert.assertEquals(0, token.getExpires());
    Assert.assertEquals(handler.getType(),
        token.getType());
    Assert.assertTrue(token.isExpired());
  }

  @SuppressWarnings("unchecked")
  private void testValidDelegationTokenHeader() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Token<DelegationTokenIdentifier> dToken =
        (Token<DelegationTokenIdentifier>) handler.getTokenManager().createToken(
            UserGroupInformation.getCurrentUser(), "user");
    Mockito.when(request.getHeader(Mockito.eq(
        DelegationTokenAuthenticator.DELEGATION_TOKEN_HEADER))).thenReturn(
        dToken.encodeToUrlString());

    AuthenticationToken token = handler.authenticate(request, response);
    Assert.assertEquals(UserGroupInformation.getCurrentUser().
        getShortUserName(), token.getUserName());
    Assert.assertEquals(0, token.getExpires());
    Assert.assertEquals(handler.getType(),
        token.getType());
    Assert.assertTrue(token.isExpired());
  }

  private void testInvalidDelegationTokenQueryString() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getQueryString()).thenReturn(
        DelegationTokenAuthenticator.DELEGATION_PARAM + "=invalid");
    StringWriter writer = new StringWriter();
    Mockito.when(response.getWriter()).thenReturn(new PrintWriter(writer));
    Assert.assertNull(handler.authenticate(request, response));
    Mockito.verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
    Assert.assertTrue(writer.toString().contains("AuthenticationException"));
  }

  private void testInvalidDelegationTokenHeader() throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getHeader(Mockito.eq(
        DelegationTokenAuthenticator.DELEGATION_TOKEN_HEADER))).thenReturn(
        "invalid");
    StringWriter writer = new StringWriter();
    Mockito.when(response.getWriter()).thenReturn(new PrintWriter(writer));
    Assert.assertNull(handler.authenticate(request, response));
    Assert.assertTrue(writer.toString().contains("AuthenticationException"));
  }

  private String getToken() throws Exception {
    DelegationTokenAuthenticator.DelegationTokenOperation op =
        DelegationTokenAuthenticator.DelegationTokenOperation.
            GETDELEGATIONTOKEN;
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getQueryString()).
        thenReturn(DelegationTokenAuthenticator.OP_PARAM + "=" + op.toString());
    Mockito.when(request.getMethod()).thenReturn(op.getHttpMethod());

    AuthenticationToken token = Mockito.mock(AuthenticationToken.class);
    Mockito.when(token.getUserName()).thenReturn("user");
    Mockito.when(response.getWriter()).thenReturn(new PrintWriter(
        new StringWriter()));
    Assert.assertFalse(handler.managementOperation(token, request, response));

    Mockito.when(request.getQueryString()).
        thenReturn(DelegationTokenAuthenticator.OP_PARAM + "=" + op.toString() +
            "&" + DelegationTokenAuthenticator.RENEWER_PARAM + "=" + null);

    Mockito.reset(response);
    Mockito.reset(token);
    Mockito.when(token.getUserName()).thenReturn("user");
    StringWriter writer = new StringWriter();
    PrintWriter pwriter = new PrintWriter(writer);
    Mockito.when(response.getWriter()).thenReturn(pwriter);
    Assert.assertFalse(handler.managementOperation(token, request, response));
    Mockito.verify(token).getUserName();
    Mockito.verify(response).setStatus(HttpServletResponse.SC_OK);
    Mockito.verify(response).setContentType(MediaType.APPLICATION_JSON);
    pwriter.close();
    String responseOutput = writer.toString();
    String tokenLabel = DelegationTokenAuthenticator.
        DELEGATION_TOKEN_JSON;
    Assert.assertTrue(responseOutput.contains(tokenLabel));
    Assert.assertTrue(responseOutput.contains(
        DelegationTokenAuthenticator.DELEGATION_TOKEN_URL_STRING_JSON));
    ObjectMapper jsonMapper = new ObjectMapper();
    Map json = jsonMapper.readValue(responseOutput, Map.class);
    json = (Map) json.get(tokenLabel);
    String tokenStr;
    tokenStr = (String) json.get(DelegationTokenAuthenticator.
        DELEGATION_TOKEN_URL_STRING_JSON);
    Token<DelegationTokenIdentifier> dt = new Token<DelegationTokenIdentifier>();
    dt.decodeFromUrlString(tokenStr);
    handler.getTokenManager().verifyToken(dt);
    return tokenStr;
  }

  @Test
  public void testCannotGetTokenUsingToken() throws Exception {
    DelegationTokenAuthenticator.DelegationTokenOperation op =
        DelegationTokenAuthenticator.DelegationTokenOperation.
            GETDELEGATIONTOKEN;
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getMethod()).thenReturn(op.getHttpMethod());
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(response.getWriter()).thenReturn(new PrintWriter(
        new StringWriter()));
    String tokenStr = getToken();
    // Try get a new token using the fetched token, should get 401.
    Mockito.when(request.getQueryString()).
        thenReturn(DelegationTokenAuthenticator.OP_PARAM + "=" + op.toString() +
            "&" + DelegationTokenAuthenticator.RENEWER_PARAM + "=" + null +
        "&" + DelegationTokenAuthenticator.DELEGATION_PARAM + "=" + tokenStr);
    Mockito.reset(response);
    StringWriter writer = new StringWriter();
    PrintWriter pwriter = new PrintWriter(writer);
    Mockito.when(response.getWriter()).thenReturn(pwriter);
    Assert.assertFalse(handler.managementOperation(null, request, response));
    Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  public void testCannotRenewTokenUsingToken() throws Exception {
    DelegationTokenAuthenticator.DelegationTokenOperation op =
        DelegationTokenAuthenticator.DelegationTokenOperation.
            RENEWDELEGATIONTOKEN;
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getMethod()).thenReturn(op.getHttpMethod());
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(response.getWriter()).thenReturn(new PrintWriter(
        new StringWriter()));
    String tokenStr = getToken();
    // Try renew a token using itself, should get 401.
    Mockito.when(request.getQueryString()).
        thenReturn(DelegationTokenAuthenticator.OP_PARAM + "=" + op.toString() +
            "&" + DelegationTokenAuthenticator.TOKEN_PARAM + "=" + tokenStr +
            "&" + DelegationTokenAuthenticator.DELEGATION_PARAM + "=" + tokenStr);
    Mockito.reset(response);
    StringWriter writer = new StringWriter();
    PrintWriter pwriter = new PrintWriter(writer);
    Mockito.when(response.getWriter()).thenReturn(pwriter);
    Assert.assertFalse(handler.managementOperation(null, request, response));
    Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  public void testWriterNotClosed() throws Exception {
    Properties conf = new Properties();
    conf.put(KerberosDelegationTokenAuthenticationHandler.TOKEN_KIND, "foo");
    conf.put(DelegationTokenAuthenticationHandler.JSON_MAPPER_PREFIX
        + "AUTO_CLOSE_TARGET", "false");
    DelegationTokenAuthenticationHandler noAuthCloseHandler =
        new MockDelegationTokenAuthenticationHandler();
    try {
      noAuthCloseHandler.initTokenManager(conf);
      noAuthCloseHandler.initJsonFactory(conf);

      DelegationTokenAuthenticator.DelegationTokenOperation op =
          GETDELEGATIONTOKEN;
      HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
      HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
      Mockito.when(request.getQueryString()).thenReturn(
          DelegationTokenAuthenticator.OP_PARAM + "=" + op.toString());
      Mockito.when(request.getMethod()).thenReturn(op.getHttpMethod());

      AuthenticationToken token = Mockito.mock(AuthenticationToken.class);
      Mockito.when(token.getUserName()).thenReturn("user");
      final MutableBoolean closed = new MutableBoolean();
      PrintWriter printWriterCloseCount = new PrintWriter(new StringWriter()) {
        @Override
        public void close() {
          closed.setValue(true);
          super.close();
        }

        @Override
        public void write(String str) {
          if (closed.booleanValue()) {
            throw new RuntimeException("already closed!");
          }
          super.write(str);
        }

      };
      Mockito.when(response.getWriter()).thenReturn(printWriterCloseCount);
      Assert.assertFalse(noAuthCloseHandler.managementOperation(token, request,
          response));
    } finally {
      noAuthCloseHandler.destroy();
    }
  }
}

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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.HttpExceptionUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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
      testNonManagementOperation();
      testManagementOperationErrors();
      testGetToken(null, new Text("foo"));
      testGetToken("bar", new Text("foo"));
      testCancelToken();
      testRenewToken();
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

  private void testGetToken(String renewer, Text expectedTokenKind)
      throws Exception {
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
        "&" + DelegationTokenAuthenticator.RENEWER_PARAM + "=" + renewer);

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
  }

  @SuppressWarnings("unchecked")
  private void testCancelToken() throws Exception {
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
    Token<DelegationTokenIdentifier> token =
        (Token<DelegationTokenIdentifier>) handler.getTokenManager().createToken(
            UserGroupInformation.getCurrentUser(), "foo");
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
  private void testRenewToken() throws Exception {
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
    Mockito.when(token.getUserName()).thenReturn("user");
    Assert.assertFalse(handler.managementOperation(token, request, response));
    Mockito.verify(response).sendError(
        Mockito.eq(HttpServletResponse.SC_BAD_REQUEST),
        Mockito.contains("requires the parameter [token]"));

    Mockito.reset(response);
    StringWriter writer = new StringWriter();
    PrintWriter pwriter = new PrintWriter(writer);
    Mockito.when(response.getWriter()).thenReturn(pwriter);
    Token<DelegationTokenIdentifier> dToken =
        (Token<DelegationTokenIdentifier>) handler.getTokenManager().createToken(
            UserGroupInformation.getCurrentUser(), "user");
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

}

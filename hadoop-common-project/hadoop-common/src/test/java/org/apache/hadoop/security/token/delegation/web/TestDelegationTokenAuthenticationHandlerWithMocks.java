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

package org.apache.hadoop.fs.http.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.http.client.HttpFSFileSystem;
import org.apache.hadoop.fs.http.client.HttpFSKerberosAuthenticator;
import org.apache.hadoop.fs.http.client.HttpFSKerberosAuthenticator.DelegationTokenOperation;
import org.apache.hadoop.hdfs.web.SWebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.lib.service.DelegationTokenIdentifier;
import org.apache.hadoop.lib.service.DelegationTokenManager;
import org.apache.hadoop.lib.service.DelegationTokenManagerException;
import org.apache.hadoop.lib.servlet.ServerWebApp;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.HFSTestCase;
import org.apache.hadoop.test.TestDir;
import org.apache.hadoop.test.TestDirHelper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class TestHttpFSKerberosAuthenticationHandler extends HFSTestCase {

  @Test
  @TestDir
  public void testManagementOperationsWebHdfsFileSystem() throws Exception {
    testManagementOperations(WebHdfsFileSystem.TOKEN_KIND);
  }

  @Test
  @TestDir
  public void testManagementOperationsSWebHdfsFileSystem() throws Exception {
    try {
      System.setProperty(HttpFSServerWebApp.NAME +
          ServerWebApp.SSL_ENABLED, "true");
      testManagementOperations(SWebHdfsFileSystem.TOKEN_KIND);
    } finally {
      System.getProperties().remove(HttpFSServerWebApp.NAME +
          ServerWebApp.SSL_ENABLED);
    }
  }

  private void testManagementOperations(Text expectedTokenKind) throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();

    Configuration httpfsConf = new Configuration(false);
    HttpFSServerWebApp server =
      new HttpFSServerWebApp(dir, dir, dir, dir, httpfsConf);
    server.setAuthority(new InetSocketAddress(InetAddress.getLocalHost(), 
                                              14000));
    AuthenticationHandler handler =
      new HttpFSKerberosAuthenticationHandlerForTesting();
    try {
      server.init();
      handler.init(null);

      testNonManagementOperation(handler);
      testManagementOperationErrors(handler);
      testGetToken(handler, null, expectedTokenKind);
      testGetToken(handler, "foo", expectedTokenKind);
      testCancelToken(handler);
      testRenewToken(handler);

    } finally {
      if (handler != null) {
        handler.destroy();
      }
    server.destroy();
    }
  }

  private void testNonManagementOperation(AuthenticationHandler handler)
    throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getParameter(HttpFSFileSystem.OP_PARAM)).
      thenReturn(null);
    Assert.assertTrue(handler.managementOperation(null, request, null));
    Mockito.when(request.getParameter(HttpFSFileSystem.OP_PARAM)).
      thenReturn(HttpFSFileSystem.Operation.CREATE.toString());
    Assert.assertTrue(handler.managementOperation(null, request, null));
  }

  private void testManagementOperationErrors(AuthenticationHandler handler)
    throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getParameter(HttpFSFileSystem.OP_PARAM)).
      thenReturn(DelegationTokenOperation.GETDELEGATIONTOKEN.toString());
    Mockito.when(request.getMethod()).thenReturn("FOO");
    Assert.assertFalse(handler.managementOperation(null, request, response));
    Mockito.verify(response).sendError(
      Mockito.eq(HttpServletResponse.SC_BAD_REQUEST),
      Mockito.startsWith("Wrong HTTP method"));

    Mockito.reset(response);
    Mockito.when(request.getMethod()).
      thenReturn(DelegationTokenOperation.GETDELEGATIONTOKEN.getHttpMethod());
    Assert.assertFalse(handler.managementOperation(null, request, response));
    Mockito.verify(response).sendError(
      Mockito.eq(HttpServletResponse.SC_UNAUTHORIZED),
      Mockito.contains("requires SPNEGO"));
  }

  private void testGetToken(AuthenticationHandler handler, String renewer,
      Text expectedTokenKind) throws Exception {
    DelegationTokenOperation op = DelegationTokenOperation.GETDELEGATIONTOKEN;
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getParameter(HttpFSFileSystem.OP_PARAM)).
      thenReturn(op.toString());
    Mockito.when(request.getMethod()).
      thenReturn(op.getHttpMethod());

    AuthenticationToken token = Mockito.mock(AuthenticationToken.class);
    Mockito.when(token.getUserName()).thenReturn("user");
    Assert.assertFalse(handler.managementOperation(null, request, response));
    Mockito.when(request.getParameter(HttpFSKerberosAuthenticator.RENEWER_PARAM)).
      thenReturn(renewer);

    Mockito.reset(response);
    StringWriter writer = new StringWriter();
    PrintWriter pwriter = new PrintWriter(writer);
    Mockito.when(response.getWriter()).thenReturn(pwriter);
    Assert.assertFalse(handler.managementOperation(token, request, response));
    if (renewer == null) {
      Mockito.verify(token).getUserName();
    } else {
      Mockito.verify(token, Mockito.never()).getUserName();
    }
    Mockito.verify(response).setStatus(HttpServletResponse.SC_OK);
    Mockito.verify(response).setContentType(MediaType.APPLICATION_JSON);
    pwriter.close();
    String responseOutput = writer.toString();
    String tokenLabel = HttpFSKerberosAuthenticator.DELEGATION_TOKEN_JSON;
    Assert.assertTrue(responseOutput.contains(tokenLabel));
    Assert.assertTrue(responseOutput.contains(
      HttpFSKerberosAuthenticator.DELEGATION_TOKEN_URL_STRING_JSON));
    JSONObject json = (JSONObject) new JSONParser().parse(responseOutput);
    json = (JSONObject) json.get(tokenLabel);
    String tokenStr;
    tokenStr = (String)
      json.get(HttpFSKerberosAuthenticator.DELEGATION_TOKEN_URL_STRING_JSON);
    Token<DelegationTokenIdentifier> dt = new Token<DelegationTokenIdentifier>();
    dt.decodeFromUrlString(tokenStr);
    HttpFSServerWebApp.get().get(DelegationTokenManager.class).verifyToken(dt);
    Assert.assertEquals(expectedTokenKind, dt.getKind());
  }

  private void testCancelToken(AuthenticationHandler handler)
    throws Exception {
    DelegationTokenOperation op =
      DelegationTokenOperation.CANCELDELEGATIONTOKEN;
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getParameter(HttpFSFileSystem.OP_PARAM)).
      thenReturn(op.toString());
    Mockito.when(request.getMethod()).
      thenReturn(op.getHttpMethod());

    Assert.assertFalse(handler.managementOperation(null, request, response));
    Mockito.verify(response).sendError(
      Mockito.eq(HttpServletResponse.SC_BAD_REQUEST),
      Mockito.contains("requires the parameter [token]"));

    Mockito.reset(response);
    Token<DelegationTokenIdentifier> token =
      HttpFSServerWebApp.get().get(DelegationTokenManager.class).createToken(
        UserGroupInformation.getCurrentUser(), "foo");
    Mockito.when(request.getParameter(HttpFSKerberosAuthenticator.TOKEN_PARAM)).
      thenReturn(token.encodeToUrlString());
    Assert.assertFalse(handler.managementOperation(null, request, response));
    Mockito.verify(response).setStatus(HttpServletResponse.SC_OK);
    try {
      HttpFSServerWebApp.get().get(DelegationTokenManager.class).verifyToken(token);
      Assert.fail();
    }
    catch (DelegationTokenManagerException ex) {
      Assert.assertTrue(ex.toString().contains("DT01"));
    }
  }

  private void testRenewToken(AuthenticationHandler handler)
    throws Exception {
    DelegationTokenOperation op =
      DelegationTokenOperation.RENEWDELEGATIONTOKEN;
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getParameter(HttpFSFileSystem.OP_PARAM)).
      thenReturn(op.toString());
    Mockito.when(request.getMethod()).
      thenReturn(op.getHttpMethod());

    Assert.assertFalse(handler.managementOperation(null, request, response));
    Mockito.verify(response).sendError(
      Mockito.eq(HttpServletResponse.SC_UNAUTHORIZED),
      Mockito.contains("equires SPNEGO authentication established"));

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
      HttpFSServerWebApp.get().get(DelegationTokenManager.class).createToken(
        UserGroupInformation.getCurrentUser(), "user");
    Mockito.when(request.getParameter(HttpFSKerberosAuthenticator.TOKEN_PARAM)).
      thenReturn(dToken.encodeToUrlString());
    Assert.assertFalse(handler.managementOperation(token, request, response));
    Mockito.verify(response).setStatus(HttpServletResponse.SC_OK);
    pwriter.close();
    Assert.assertTrue(writer.toString().contains("long"));
    HttpFSServerWebApp.get().get(DelegationTokenManager.class).verifyToken(dToken);
  }

  @Test
  @TestDir
  public void testAuthenticate() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();

    Configuration httpfsConf = new Configuration(false);
    HttpFSServerWebApp server =
      new HttpFSServerWebApp(dir, dir, dir, dir, httpfsConf);
    server.setAuthority(new InetSocketAddress(InetAddress.getLocalHost(),
                                              14000));
    AuthenticationHandler handler =
      new HttpFSKerberosAuthenticationHandlerForTesting();
    try {
      server.init();
      handler.init(null);

      testValidDelegationToken(handler);
      testInvalidDelegationToken(handler);
    } finally {
      if (handler != null) {
        handler.destroy();
      }
    server.destroy();
    }
  }

  private void testValidDelegationToken(AuthenticationHandler handler)
    throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Token<DelegationTokenIdentifier> dToken =
      HttpFSServerWebApp.get().get(DelegationTokenManager.class).createToken(
        UserGroupInformation.getCurrentUser(), "user");
    Mockito.when(request.getParameter(HttpFSKerberosAuthenticator.DELEGATION_PARAM)).
      thenReturn(dToken.encodeToUrlString());

    AuthenticationToken token = handler.authenticate(request, response);
    Assert.assertEquals(UserGroupInformation.getCurrentUser().getShortUserName(),
                        token.getUserName());
    Assert.assertEquals(0, token.getExpires());
    Assert.assertEquals(HttpFSKerberosAuthenticationHandler.TYPE,
                        token.getType());
    Assert.assertTrue(token.isExpired());
  }

  private void testInvalidDelegationToken(AuthenticationHandler handler)
    throws Exception {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    Mockito.when(request.getParameter(HttpFSKerberosAuthenticator.DELEGATION_PARAM)).
      thenReturn("invalid");

    try {
      handler.authenticate(request, response);
      Assert.fail();
    } catch (AuthenticationException ex) {
      //NOP
    } catch (Exception ex) {
      Assert.fail();
    }
  }

}

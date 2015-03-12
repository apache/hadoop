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
package org.apache.hadoop.hdfs.server.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NameNodeHttpServer;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.DefaultImpersonationProvider;
import org.apache.hadoop.security.authorize.ProxyServers;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.Assert;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestJspHelper {

  private final Configuration conf = new HdfsConfiguration();

  // allow user with TGT to run tests
  @BeforeClass
  public static void setupKerb() {
    System.setProperty("java.security.krb5.kdc", "");
    System.setProperty("java.security.krb5.realm", "NONE");
  }    

  public static class DummySecretManager extends
      AbstractDelegationTokenSecretManager<DelegationTokenIdentifier> {

    public DummySecretManager(long delegationKeyUpdateInterval,
        long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
        long delegationTokenRemoverScanInterval) {
      super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
          delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    }

    @Override
    public DelegationTokenIdentifier createIdentifier() {
      return null;
    }

    @Override
    public byte[] createPassword(DelegationTokenIdentifier dtId) {
      return new byte[1];
    }
  }

  @Test
  public void testGetUgi() throws IOException {
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "hdfs://localhost:4321/");
    HttpServletRequest request = mock(HttpServletRequest.class);
    ServletContext context = mock(ServletContext.class);
    String user = "TheDoctor";
    Text userText = new Text(user);
    DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(userText,
        userText, null);
    Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>(
        dtId, new DummySecretManager(0, 0, 0, 0));
    String tokenString = token.encodeToUrlString();
    when(request.getParameter(JspHelper.DELEGATION_PARAMETER_NAME)).thenReturn(
        tokenString);
    when(request.getRemoteUser()).thenReturn(user);

    //Test attribute in the url to be used as service in the token.
    when(request.getParameter(JspHelper.NAMENODE_ADDRESS)).thenReturn(
        "1.1.1.1:1111");

    conf.set(DFSConfigKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);

    verifyServiceInToken(context, request, "1.1.1.1:1111");
    
    //Test attribute name.node.address 
    //Set the nnaddr url parameter to null.
    when(request.getParameter(JspHelper.NAMENODE_ADDRESS)).thenReturn(null);
    InetSocketAddress addr = new InetSocketAddress("localhost", 2222);
    when(context.getAttribute(NameNodeHttpServer.NAMENODE_ADDRESS_ATTRIBUTE_KEY))
        .thenReturn(addr);
    verifyServiceInToken(context, request, addr.getAddress().getHostAddress()
        + ":2222");
    
    //Test service already set in the token
    token.setService(new Text("3.3.3.3:3333"));
    tokenString = token.encodeToUrlString();
    //Set the name.node.address attribute in Servlet context to null
    when(context.getAttribute(NameNodeHttpServer.NAMENODE_ADDRESS_ATTRIBUTE_KEY))
        .thenReturn(null);
    when(request.getParameter(JspHelper.DELEGATION_PARAMETER_NAME)).thenReturn(
        tokenString);
    verifyServiceInToken(context, request, "3.3.3.3:3333");
  }
  
  private void verifyServiceInToken(ServletContext context,
      HttpServletRequest request, String expected) throws IOException {
    UserGroupInformation ugi = JspHelper.getUGI(context, request, conf);
    Token<? extends TokenIdentifier> tokenInUgi = ugi.getTokens().iterator()
        .next();
    Assert.assertEquals(expected, tokenInUgi.getService().toString());
  }
  

  @Test
  public void testGetUgiFromToken() throws IOException {
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "hdfs://localhost:4321/");
    ServletContext context = mock(ServletContext.class);
    String realUser = "TheDoctor";
    String user = "TheNurse";
    conf.set(DFSConfigKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation ugi;
    HttpServletRequest request;
    
    Text ownerText = new Text(user);
    DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(
        ownerText, ownerText, new Text(realUser));
    Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>(
        dtId, new DummySecretManager(0, 0, 0, 0));
    String tokenString = token.encodeToUrlString();
    
    // token with no auth-ed user
    request = getMockRequest(null, null, null);
    when(request.getParameter(JspHelper.DELEGATION_PARAMETER_NAME)).thenReturn(
        tokenString);
    ugi = JspHelper.getUGI(context, request, conf);
    Assert.assertNotNull(ugi.getRealUser());
    Assert.assertEquals(ugi.getRealUser().getShortUserName(), realUser);
    Assert.assertEquals(ugi.getShortUserName(), user);
    checkUgiFromToken(ugi);
    
    // token with auth-ed user
    request = getMockRequest(realUser, null, null);
    when(request.getParameter(JspHelper.DELEGATION_PARAMETER_NAME)).thenReturn(
        tokenString);
    ugi = JspHelper.getUGI(context, request, conf);
    Assert.assertNotNull(ugi.getRealUser());
    Assert.assertEquals(ugi.getRealUser().getShortUserName(), realUser);
    Assert.assertEquals(ugi.getShortUserName(), user);    
    checkUgiFromToken(ugi);
    
    // completely different user, token trumps auth
    request = getMockRequest("rogue", null, null);
    when(request.getParameter(JspHelper.DELEGATION_PARAMETER_NAME)).thenReturn(
        tokenString);
    ugi = JspHelper.getUGI(context, request, conf);
    Assert.assertNotNull(ugi.getRealUser());
    Assert.assertEquals(ugi.getRealUser().getShortUserName(), realUser);
    Assert.assertEquals(ugi.getShortUserName(), user);    
    checkUgiFromToken(ugi);
    
    // expected case
    request = getMockRequest(null, user, null);
    when(request.getParameter(JspHelper.DELEGATION_PARAMETER_NAME)).thenReturn(
        tokenString);
    ugi = JspHelper.getUGI(context, request, conf);
    Assert.assertNotNull(ugi.getRealUser());
    Assert.assertEquals(ugi.getRealUser().getShortUserName(), realUser);
    Assert.assertEquals(ugi.getShortUserName(), user);    
    checkUgiFromToken(ugi);
    
    // can't proxy with a token!
    request = getMockRequest(null, null, "rogue");
    when(request.getParameter(JspHelper.DELEGATION_PARAMETER_NAME)).thenReturn(
        tokenString);
    try {
      JspHelper.getUGI(context, request, conf);
      Assert.fail("bad request allowed");
    } catch (IOException ioe) {
      Assert.assertEquals(
          "Usernames not matched: name=rogue != expected="+user,
          ioe.getMessage());
    }
    
    // can't proxy with a token!
    request = getMockRequest(null, user, "rogue");
    when(request.getParameter(JspHelper.DELEGATION_PARAMETER_NAME)).thenReturn(
        tokenString);
    try {
      JspHelper.getUGI(context, request, conf);
      Assert.fail("bad request allowed");
    } catch (IOException ioe) {
      Assert.assertEquals(
          "Usernames not matched: name=rogue != expected="+user,
          ioe.getMessage());
    }
  }
  
  @Test
  public void testGetNonProxyUgi() throws IOException {
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "hdfs://localhost:4321/");
    ServletContext context = mock(ServletContext.class);
    String realUser = "TheDoctor";
    String user = "TheNurse";
    conf.set(DFSConfigKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation ugi;
    HttpServletRequest request;
    
    // have to be auth-ed with remote user
    request = getMockRequest(null, null, null);
    try {
      JspHelper.getUGI(context, request, conf);
      Assert.fail("bad request allowed");
    } catch (IOException ioe) {
      Assert.assertEquals(
          "Security enabled but user not authenticated by filter",
          ioe.getMessage());
    }
    request = getMockRequest(null, realUser, null);
    try {
      JspHelper.getUGI(context, request, conf);
      Assert.fail("bad request allowed");
    } catch (IOException ioe) {
      Assert.assertEquals(
          "Security enabled but user not authenticated by filter",
          ioe.getMessage());
    }
    
    // ugi for remote user
    request = getMockRequest(realUser, null, null);
    ugi = JspHelper.getUGI(context, request, conf);
    Assert.assertNull(ugi.getRealUser());
    Assert.assertEquals(ugi.getShortUserName(), realUser);
    checkUgiFromAuth(ugi);
    
    // ugi for remote user = real user
    request = getMockRequest(realUser, realUser, null);
    ugi = JspHelper.getUGI(context, request, conf);
    Assert.assertNull(ugi.getRealUser());
    Assert.assertEquals(ugi.getShortUserName(), realUser);
    checkUgiFromAuth(ugi);
    
    // ugi for remote user != real user 
    request = getMockRequest(realUser, user, null);
    try {
      JspHelper.getUGI(context, request, conf);
      Assert.fail("bad request allowed");
    } catch (IOException ioe) {
      Assert.assertEquals(
          "Usernames not matched: name="+user+" != expected="+realUser,
          ioe.getMessage());
    }
  }
  
  @Test
  public void testGetProxyUgi() throws IOException {
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "hdfs://localhost:4321/");
    ServletContext context = mock(ServletContext.class);
    String realUser = "TheDoctor";
    String user = "TheNurse";
    conf.set(DFSConfigKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    
    conf.set(DefaultImpersonationProvider.getTestProvider().
        getProxySuperuserGroupConfKey(realUser), "*");
    conf.set(DefaultImpersonationProvider.getTestProvider().
        getProxySuperuserIpConfKey(realUser), "*");
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation ugi;
    HttpServletRequest request;
    
    // have to be auth-ed with remote user
    request = getMockRequest(null, null, user);
    try {
      JspHelper.getUGI(context, request, conf);
      Assert.fail("bad request allowed");
    } catch (IOException ioe) {
      Assert.assertEquals(
          "Security enabled but user not authenticated by filter",
          ioe.getMessage());
    }
    request = getMockRequest(null, realUser, user);
    try {
      JspHelper.getUGI(context, request, conf);
      Assert.fail("bad request allowed");
    } catch (IOException ioe) {
      Assert.assertEquals(
          "Security enabled but user not authenticated by filter",
          ioe.getMessage());
    }
    
    // proxy ugi for user via remote user
    request = getMockRequest(realUser, null, user);
    ugi = JspHelper.getUGI(context, request, conf);
    Assert.assertNotNull(ugi.getRealUser());
    Assert.assertEquals(ugi.getRealUser().getShortUserName(), realUser);
    Assert.assertEquals(ugi.getShortUserName(), user);
    checkUgiFromAuth(ugi);
    
    // proxy ugi for user vi a remote user = real user
    request = getMockRequest(realUser, realUser, user);
    ugi = JspHelper.getUGI(context, request, conf);
    Assert.assertNotNull(ugi.getRealUser());
    Assert.assertEquals(ugi.getRealUser().getShortUserName(), realUser);
    Assert.assertEquals(ugi.getShortUserName(), user);
    checkUgiFromAuth(ugi);
    
    // proxy ugi for user via remote user != real user
    request = getMockRequest(realUser, user, user);
    try {
      JspHelper.getUGI(context, request, conf);
      Assert.fail("bad request allowed");
    } catch (IOException ioe) {
      Assert.assertEquals(
          "Usernames not matched: name="+user+" != expected="+realUser,
          ioe.getMessage());
    }
    
    // try to get get a proxy user with unauthorized user
    try {
      request = getMockRequest(user, null, realUser);
      JspHelper.getUGI(context, request, conf);
      Assert.fail("bad proxy request allowed");
    } catch (AuthorizationException ae) {
      Assert.assertEquals(
          "User: " + user + " is not allowed to impersonate " + realUser,
           ae.getMessage());
    }
    try {
      request = getMockRequest(user, user, realUser);
      JspHelper.getUGI(context, request, conf);
      Assert.fail("bad proxy request allowed");
    } catch (AuthorizationException ae) {
      Assert.assertEquals(
          "User: " + user + " is not allowed to impersonate " + realUser,
           ae.getMessage());
    }
  }

  private HttpServletRequest getMockRequest(String remoteUser, String user, String doAs) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getParameter(UserParam.NAME)).thenReturn(user);
    if (doAs != null) {
      when(request.getParameter(DoAsParam.NAME)).thenReturn(doAs);
    }
    when(request.getRemoteUser()).thenReturn(remoteUser);
    return request;
  }
  
  private void checkUgiFromAuth(UserGroupInformation ugi) {
    if (ugi.getRealUser() != null) {
      Assert.assertEquals(AuthenticationMethod.PROXY,
                          ugi.getAuthenticationMethod());
      Assert.assertEquals(AuthenticationMethod.KERBEROS_SSL,
                          ugi.getRealUser().getAuthenticationMethod());
    } else {
      Assert.assertEquals(AuthenticationMethod.KERBEROS_SSL,
                          ugi.getAuthenticationMethod()); 
    }
  }
  
  private void checkUgiFromToken(UserGroupInformation ugi) {
    if (ugi.getRealUser() != null) {
      Assert.assertEquals(AuthenticationMethod.PROXY,
                          ugi.getAuthenticationMethod());
      Assert.assertEquals(AuthenticationMethod.TOKEN,
                          ugi.getRealUser().getAuthenticationMethod());
    } else {
      Assert.assertEquals(AuthenticationMethod.TOKEN,
                          ugi.getAuthenticationMethod());
    }
  }

  @Test
  public void testReadWriteReplicaState() {
    try {
      DataOutputBuffer out = new DataOutputBuffer();
      DataInputBuffer in = new DataInputBuffer();
      for (HdfsServerConstants.ReplicaState repState : HdfsServerConstants.ReplicaState
          .values()) {
        repState.write(out);
        in.reset(out.getData(), out.getLength());
        HdfsServerConstants.ReplicaState result = HdfsServerConstants.ReplicaState
            .read(in);
        assertTrue("testReadWrite error !!!", repState == result);
        out.reset();
        in.reset();
      }
    } catch (Exception ex) {
      fail("testReadWrite ex error ReplicaState");
    }
  }
 
  private static String clientAddr = "1.1.1.1";
  private static String chainedClientAddr = clientAddr+", 2.2.2.2";
  private static String proxyAddr = "3.3.3.3";
  
  @Test
  public void testRemoteAddr() {
    assertEquals(clientAddr, getRemoteAddr(clientAddr, null, false));
  }
  
  @Test
  public void testRemoteAddrWithUntrustedProxy() {
    assertEquals(proxyAddr, getRemoteAddr(clientAddr, proxyAddr, false));
  }

  @Test
  public void testRemoteAddrWithTrustedProxy() {
    assertEquals(clientAddr, getRemoteAddr(clientAddr, proxyAddr, true));
    assertEquals(clientAddr, getRemoteAddr(chainedClientAddr, proxyAddr, true));
  }

  @Test
  public void testRemoteAddrWithTrustedProxyAndEmptyClient() {
    assertEquals(proxyAddr, getRemoteAddr(null, proxyAddr, true));
    assertEquals(proxyAddr, getRemoteAddr("", proxyAddr, true));
  }

  private String getRemoteAddr(String clientAddr, String proxyAddr, boolean trusted) {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getRemoteAddr()).thenReturn("1.2.3.4");

    Configuration conf = new Configuration();
    if (proxyAddr == null) {
      when(req.getRemoteAddr()).thenReturn(clientAddr);
    } else {
      when(req.getRemoteAddr()).thenReturn(proxyAddr);
      when(req.getHeader("X-Forwarded-For")).thenReturn(clientAddr);
      if (trusted) {
        conf.set(ProxyServers.CONF_HADOOP_PROXYSERVERS, proxyAddr);
      }
    }
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    return JspHelper.getRemoteAddr(req);
  }
}


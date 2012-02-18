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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NameNodeHttpServer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.junit.Assert;
import org.junit.Test;

public class TestJspHelper {

  private Configuration conf = new HdfsConfiguration();

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
    Assert.assertEquals(tokenInUgi.getService().toString(), expected);
  }
  
  
  @Test
  public void testDelegationTokenUrlParam() {
    conf.set(DFSConfigKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    String tokenString = "xyzabc";
    String delegationTokenParam = JspHelper
        .getDelegationTokenUrlParam(tokenString);
    //Security is enabled
    Assert.assertEquals(JspHelper.SET_DELEGATION + "xyzabc",
        delegationTokenParam);
    conf.set(DFSConfigKeys.HADOOP_SECURITY_AUTHENTICATION, "simple");
    UserGroupInformation.setConfiguration(conf);
    delegationTokenParam = JspHelper
        .getDelegationTokenUrlParam(tokenString);
    //Empty string must be returned because security is disabled.
    Assert.assertEquals("", delegationTokenParam);
  }

}

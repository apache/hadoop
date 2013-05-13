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

package org.apache.hadoop.hdfs.web;

import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegationTokenRenewer;
import org.apache.hadoop.fs.DelegationTokenRenewer.RenewAction;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.resources.DeleteOpParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestWebHdfsTokens {
  static Configuration conf;
  static UserGroupInformation ugi;
  
  @BeforeClass
  public static void setup() throws IOException {
    conf = new Configuration();
    SecurityUtil.setAuthenticationMethod(KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);    
    ugi = UserGroupInformation.getCurrentUser();
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout=1000)
  public void testInitWithNoToken() throws IOException {
    WebHdfsFileSystem fs = spy(new WebHdfsFileSystem());
    doReturn(null).when(fs).getDelegationToken(anyString());
    doNothing().when(fs).addRenewAction(any(WebHdfsFileSystem.class));
    fs.initialize(URI.create("webhdfs://127.0.0.1:0"), conf);
    
    // when not in ugi, don't get one
    verify(fs).initDelegationToken();
    verify(fs).selectDelegationToken(ugi);
    verify(fs, never()).setDelegationToken(any(Token.class));
    verify(fs, never()).getDelegationToken();
    verify(fs, never()).getDelegationToken(anyString());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout=1000)
  public void testInitWithUGIToken() throws IOException {
    WebHdfsFileSystem fs = spy(new WebHdfsFileSystem());
    Token<DelegationTokenIdentifier> token = mock(Token.class);    
    doReturn(token).when(fs).selectDelegationToken(ugi);
    doReturn(null).when(fs).getDelegationToken(anyString());
    doNothing().when(fs).addRenewAction(any(WebHdfsFileSystem.class));
    fs.initialize(URI.create("webhdfs://127.0.0.1:0"), conf);
    
    // when in the ugi, store it but don't renew it
    verify(fs).initDelegationToken();
    verify(fs).selectDelegationToken(ugi);
    verify(fs).setDelegationToken(token);
    verify(fs, never()).getDelegationToken();
    verify(fs, never()).getDelegationToken(anyString());
    verify(fs, never()).addRenewAction(fs);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout=1000)
  public void testInternalGetDelegationToken() throws IOException {
    WebHdfsFileSystem fs = spy(new WebHdfsFileSystem());
    Token<DelegationTokenIdentifier> token = mock(Token.class);    
    doReturn(null).when(fs).selectDelegationToken(ugi);
    doReturn(token).when(fs).getDelegationToken(anyString());
    doNothing().when(fs).addRenewAction(any(WebHdfsFileSystem.class));
    fs.initialize(URI.create("webhdfs://127.0.0.1:0"), conf);

    // get token, store it, and renew it
    Token<?> token2 = fs.getDelegationToken();
    assertEquals(token2, token);
    verify(fs).getDelegationToken(null);
    verify(fs).setDelegationToken(token);
    verify(fs).addRenewAction(fs);
    reset(fs);

    // just return token, don't get/set/renew
    token2 = fs.getDelegationToken();
    assertEquals(token2, token);
    verify(fs, never()).getDelegationToken(null);
    verify(fs, never()).setDelegationToken(any(Token.class));
    verify(fs, never()).addRenewAction(fs);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout=1000)
  public void testTokenForNonTokenOp() throws IOException {
    WebHdfsFileSystem fs = spy(new WebHdfsFileSystem());
    Token<DelegationTokenIdentifier> token = mock(Token.class);    
    doReturn(null).when(fs).selectDelegationToken(ugi);
    doReturn(token).when(fs).getDelegationToken(null);
    doNothing().when(fs).addRenewAction(any(WebHdfsFileSystem.class));
    fs.initialize(URI.create("webhdfs://127.0.0.1:0"), conf);

    // should get/set/renew token
    fs.toUrl(GetOpParam.Op.OPEN, null);
    verify(fs).getDelegationToken();
    verify(fs).getDelegationToken(null);
    verify(fs).setDelegationToken(token);
    verify(fs).addRenewAction(fs);
    reset(fs);
    
    // should return prior token
    fs.toUrl(GetOpParam.Op.OPEN, null);
    verify(fs).getDelegationToken();
    verify(fs, never()).getDelegationToken(null);
    verify(fs, never()).setDelegationToken(token);
    verify(fs, never()).addRenewAction(fs);
  }
  
  @Test(timeout=1000)
  public void testNoTokenForGetToken() throws IOException {
    checkNoTokenForOperation(GetOpParam.Op.GETDELEGATIONTOKEN);
  }
  
  @Test(timeout=1000)
  public void testNoTokenForCanclToken() throws IOException {
    checkNoTokenForOperation(PutOpParam.Op.RENEWDELEGATIONTOKEN);
  }

  @Test(timeout=1000)
  public void testNoTokenForCancelToken() throws IOException {
    checkNoTokenForOperation(PutOpParam.Op.CANCELDELEGATIONTOKEN);
  }

  @SuppressWarnings("unchecked")
  private void checkNoTokenForOperation(HttpOpParam.Op op) throws IOException {
    WebHdfsFileSystem fs = spy(new WebHdfsFileSystem());
    doReturn(null).when(fs).selectDelegationToken(ugi);
    doReturn(null).when(fs).getDelegationToken(null);
    doNothing().when(fs).addRenewAction(any(WebHdfsFileSystem.class));
    fs.initialize(URI.create("webhdfs://127.0.0.1:0"), conf);
    
    // do not get a token!
    fs.toUrl(op, null);
    verify(fs, never()).getDelegationToken();
    verify(fs, never()).getDelegationToken(null);
    verify(fs, never()).setDelegationToken(any(Token.class));
    verify(fs, never()).addRenewAction(fs);
  }
  
  @Test(timeout=1000)
  public void testGetOpRequireAuth() {
    for (HttpOpParam.Op op : GetOpParam.Op.values()) {
      boolean expect = (op == GetOpParam.Op.GETDELEGATIONTOKEN);
      assertEquals(expect, op.getRequireAuth()); 
    }
  }

  @Test(timeout=1000)
  public void testPutOpRequireAuth() {
    for (HttpOpParam.Op op : PutOpParam.Op.values()) {
      boolean expect = (op == PutOpParam.Op.RENEWDELEGATIONTOKEN ||
                        op == PutOpParam.Op.CANCELDELEGATIONTOKEN);
      assertEquals(expect, op.getRequireAuth()); 
    }
  }
  
  @Test(timeout=1000)
  public void testPostOpRequireAuth() {    
    for (HttpOpParam.Op op : PostOpParam.Op.values()) {
      assertFalse(op.getRequireAuth());
    }
  }
  
  @Test(timeout=1000)
  public void testDeleteOpRequireAuth() {    
    for (HttpOpParam.Op op : DeleteOpParam.Op.values()) {
      assertFalse(op.getRequireAuth());
    }
  }
  
  @Test
  public void testGetTokenAfterFailure() throws Exception {
    Configuration conf = mock(Configuration.class);
    Token<?> token1 = mock(Token.class);
    Token<?> token2 = mock(Token.class);
    long renewCycle = 1000;
    
    DelegationTokenRenewer.renewCycle = renewCycle;
    WebHdfsFileSystem fs = spy(new WebHdfsFileSystem());
    doReturn(conf).when(fs).getConf();
    doReturn(token1).doReturn(token2).when(fs).getDelegationToken(null);
    // cause token renewer to abandon the token
    doThrow(new IOException("renew failed")).when(token1).renew(conf);
    doThrow(new IOException("get failed")).when(fs).addDelegationTokens(null, null);

    // trigger token acquisition
    Token<?> token = fs.getDelegationToken();
    RenewAction<?> action = fs.action; 
    assertSame(token1, token);
    assertTrue(action.isValid());

    // fetch again and make sure it's the same as before
    token = fs.getDelegationToken();
    assertSame(token1, token);
    assertSame(action, fs.action);
    assertTrue(fs.action.isValid());
    
    // upon renewal, token will go bad based on above stubbing
    Thread.sleep(renewCycle);
    assertSame(action, fs.action);
    assertFalse(fs.action.isValid());
    
    // now that token is invalid, should get a new one
    token = fs.getDelegationToken();
    assertSame(token2, token);
    assertNotSame(action, fs.action);
    assertTrue(fs.action.isValid());
    action = fs.action;
    
    // should get same one again
    token = fs.getDelegationToken();
    assertSame(token2, token);
    assertSame(action, fs.action);
    assertTrue(fs.action.isValid());
  }
}

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
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
import org.mockito.internal.util.reflection.Whitebox;

public class TestWebHdfsTokens {
  private static Configuration conf;

  @BeforeClass
  public static void setUp() {
    conf = new Configuration();
    SecurityUtil.setAuthenticationMethod(KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);    
  }

  private WebHdfsFileSystem spyWebhdfsInSecureSetup() throws IOException {
    WebHdfsFileSystem fsOrig = new WebHdfsFileSystem();
    fsOrig.initialize(URI.create("webhdfs://127.0.0.1:0"), conf);
    WebHdfsFileSystem fs = spy(fsOrig);
    Whitebox.setInternalState(fsOrig.tokenAspect, "fs", fs);
    return fs;
  }

  @Test(timeout = 5000)
  public void testTokenForNonTokenOp() throws IOException {
    WebHdfsFileSystem fs = spyWebhdfsInSecureSetup();
    Token<?> token = mock(Token.class);
    doReturn(token).when(fs).getDelegationToken(null);

    // should get/set/renew token
    fs.toUrl(GetOpParam.Op.OPEN, null);
    verify(fs).getDelegationToken();
    verify(fs).getDelegationToken(null);
    verify(fs).setDelegationToken(token);
    reset(fs);

    // should return prior token
    fs.toUrl(GetOpParam.Op.OPEN, null);
    verify(fs).getDelegationToken();
    verify(fs, never()).getDelegationToken(null);
    verify(fs, never()).setDelegationToken(token);
  }

  @Test(timeout = 5000)
  public void testNoTokenForGetToken() throws IOException {
    checkNoTokenForOperation(GetOpParam.Op.GETDELEGATIONTOKEN);
  }

  @Test(timeout = 5000)
  public void testNoTokenForCanclToken() throws IOException {
    checkNoTokenForOperation(PutOpParam.Op.RENEWDELEGATIONTOKEN);
  }

  @Test(timeout = 5000)
  public void testNoTokenForCancelToken() throws IOException {
    checkNoTokenForOperation(PutOpParam.Op.CANCELDELEGATIONTOKEN);
  }

  private void checkNoTokenForOperation(HttpOpParam.Op op) throws IOException {
    WebHdfsFileSystem fs = spyWebhdfsInSecureSetup();
    doReturn(null).when(fs).getDelegationToken(null);
    fs.initialize(URI.create("webhdfs://127.0.0.1:0"), conf);

    // do not get a token!
    fs.toUrl(op, null);
    verify(fs, never()).getDelegationToken();
    verify(fs, never()).getDelegationToken(null);
    verify(fs, never()).setDelegationToken((Token<?>)any(Token.class));
  }

  @Test(timeout = 1000)
  public void testGetOpRequireAuth() {
    for (HttpOpParam.Op op : GetOpParam.Op.values()) {
      boolean expect = (op == GetOpParam.Op.GETDELEGATIONTOKEN);
      assertEquals(expect, op.getRequireAuth());
    }
  }

  @Test(timeout = 1000)
  public void testPutOpRequireAuth() {
    for (HttpOpParam.Op op : PutOpParam.Op.values()) {
      boolean expect = (op == PutOpParam.Op.RENEWDELEGATIONTOKEN || op == PutOpParam.Op.CANCELDELEGATIONTOKEN);
      assertEquals(expect, op.getRequireAuth());
    }
  }

  @Test(timeout = 1000)
  public void testPostOpRequireAuth() {
    for (HttpOpParam.Op op : PostOpParam.Op.values()) {
      assertFalse(op.getRequireAuth());
    }
  }

  @Test(timeout = 1000)
  public void testDeleteOpRequireAuth() {
    for (HttpOpParam.Op op : DeleteOpParam.Op.values()) {
      assertFalse(op.getRequireAuth());
    }
  }
}

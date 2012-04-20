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

package org.apache.hadoop.hdfs;

import static 
  org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION;

import java.lang.reflect.Field;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtilTestHelper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

public class TestHftpDelegationToken {

  @Test
  public void testHdfsDelegationToken() throws Exception {
    final Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation user =  
      UserGroupInformation.createUserForTesting("oom", 
                                                new String[]{"memory"});
    Token<?> token = new Token<TokenIdentifier>
      (new byte[0], new byte[0], 
       DelegationTokenIdentifier.HDFS_DELEGATION_KIND,
       new Text("127.0.0.1:8020"));
    user.addToken(token);
    Token<?> token2 = new Token<TokenIdentifier>
      (null, null, new Text("other token"), new Text("127.0.0.1:8020"));
    user.addToken(token2);
    assertEquals("wrong tokens in user", 2, user.getTokens().size());
    FileSystem fs = 
      user.doAs(new PrivilegedExceptionAction<FileSystem>() {
	  public FileSystem run() throws Exception {
            return FileSystem.get(new URI("hftp://localhost:50470/"), conf);
	  }
	});
    assertSame("wrong kind of file system", HftpFileSystem.class,
                 fs.getClass());
    Field renewToken = HftpFileSystem.class.getDeclaredField("renewToken");
    renewToken.setAccessible(true);
    assertSame("wrong token", token, renewToken.get(fs));
  }

  @Test
  public void testSelectHdfsDelegationToken() throws Exception {
    SecurityUtilTestHelper.setTokenServiceUseIp(true);

    Configuration conf = new Configuration();
    URI hftpUri = URI.create("hftp://localhost:0");
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    Token<?> token = null;
    
    // test fallback to hdfs token
    Token<?> hdfsToken = new Token<TokenIdentifier>(
        new byte[0], new byte[0],
        DelegationTokenIdentifier.HDFS_DELEGATION_KIND,
        new Text("127.0.0.1:8020"));
    ugi.addToken(hdfsToken);
    
    HftpFileSystem fs = (HftpFileSystem) FileSystem.get(hftpUri, conf);
    token = fs.selectDelegationToken();
    assertNotNull(token);
    assertEquals(hdfsToken, token);
    
    // test hftp is favored over hdfs
    Token<?> hftpToken = new Token<TokenIdentifier>(
        new byte[0], new byte[0],
        HftpFileSystem.TOKEN_KIND, new Text("127.0.0.1:0"));
    ugi.addToken(hftpToken);
    token = fs.selectDelegationToken();
    assertNotNull(token);
    assertEquals(hftpToken, token);
    
    // switch to using host-based tokens, no token should match
    SecurityUtilTestHelper.setTokenServiceUseIp(false);
    token = fs.selectDelegationToken();
    assertNull(token);
    
    // test fallback to hdfs token
    hdfsToken = new Token<TokenIdentifier>(
        new byte[0], new byte[0],
        DelegationTokenIdentifier.HDFS_DELEGATION_KIND,
        new Text("localhost:8020"));
    ugi.addToken(hdfsToken);
    token = fs.selectDelegationToken();
    assertNotNull(token);
    assertEquals(hdfsToken, token);

    // test hftp is favored over hdfs
    hftpToken = new Token<TokenIdentifier>(
        new byte[0], new byte[0],
        HftpFileSystem.TOKEN_KIND, new Text("localhost:0"));
    ugi.addToken(hftpToken);
    token = fs.selectDelegationToken();
    assertNotNull(token);
    assertEquals(hftpToken, token);
  }
}
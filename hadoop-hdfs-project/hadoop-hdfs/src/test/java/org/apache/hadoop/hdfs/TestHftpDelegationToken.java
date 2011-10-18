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

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.net.URI;
import java.net.URL;
import java.net.HttpURLConnection;
import java.security.PrivilegedExceptionAction;
import java.util.Random;

import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import static org.junit.Assert.*;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
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
}
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

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

public class TestHftpDelegationToken {

  /**
   * Test whether HftpFileSystem maintain wire-compatibility for 0.20.203 when
   * obtaining delegation token. See HDFS-5440 for more details.
   */
  @Test
  public void testTokenCompatibilityFor203() throws IOException,
      URISyntaxException, AuthenticationException {
    Configuration conf = new Configuration();
    HftpFileSystem fs = new HftpFileSystem();

    Token<?> token = new Token<TokenIdentifier>(new byte[0], new byte[0],
        DelegationTokenIdentifier.HDFS_DELEGATION_KIND, new Text(
            "127.0.0.1:8020"));
    Credentials cred = new Credentials();
    cred.addToken(HftpFileSystem.TOKEN_KIND, token);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    cred.write(new DataOutputStream(os));

    HttpURLConnection conn = mock(HttpURLConnection.class);
    doReturn(new ByteArrayInputStream(os.toByteArray())).when(conn)
        .getInputStream();
    doReturn(HttpURLConnection.HTTP_OK).when(conn).getResponseCode();

    URLConnectionFactory factory = mock(URLConnectionFactory.class);
    doReturn(conn).when(factory).openConnection(Mockito.<URL> any(),
        anyBoolean());

    final URI uri = new URI("hftp://127.0.0.1:8020");
    fs.initialize(uri, conf);
    fs.connectionFactory = factory;

    UserGroupInformation ugi = UserGroupInformation.createUserForTesting("foo",
        new String[] { "bar" });

    TokenAspect<HftpFileSystem> tokenAspect = new TokenAspect<HftpFileSystem>(
        fs, SecurityUtil.buildTokenService(uri), HftpFileSystem.TOKEN_KIND);

    tokenAspect.initDelegationToken(ugi);
    tokenAspect.ensureTokenInitialized();

    Assert.assertSame(HftpFileSystem.TOKEN_KIND, fs.getRenewToken().getKind());

    Token<?> tok = (Token<?>) Whitebox.getInternalState(fs, "delegationToken");
    Assert.assertNotSame("Not making a copy of the remote token", token, tok);
    Assert.assertEquals(token.getKind(), tok.getKind());
  }
}

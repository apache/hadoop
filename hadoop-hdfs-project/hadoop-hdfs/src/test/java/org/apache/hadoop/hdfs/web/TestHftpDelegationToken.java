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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.HftpFileSystem;
import org.apache.hadoop.hdfs.web.HsftpFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtilTestHelper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Test;

public class TestHftpDelegationToken {

  @Test
  public void testHdfsDelegationToken() throws Exception {
    SecurityUtilTestHelper.setTokenServiceUseIp(true);

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
      (null, null, new Text("other token"), new Text("127.0.0.1:8021"));
    user.addToken(token2);
    assertEquals("wrong tokens in user", 2, user.getTokens().size());
    FileSystem fs =
      user.doAs(new PrivilegedExceptionAction<FileSystem>() {
	  @Override
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
  public void testSelectHftpDelegationToken() throws Exception {
    SecurityUtilTestHelper.setTokenServiceUseIp(true);

    Configuration conf = new Configuration();
    conf.setClass("fs.hftp.impl", MyHftpFileSystem.class, FileSystem.class);

    int httpPort = 80;
    int httpsPort = 443;
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY, httpPort);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY, httpsPort);

    // test with implicit default port
    URI fsUri = URI.create("hftp://localhost");
    MyHftpFileSystem fs = (MyHftpFileSystem) FileSystem.newInstance(fsUri, conf);
    assertEquals(httpPort, fs.getCanonicalUri().getPort());
    checkTokenSelection(fs, httpPort, conf);

    // test with explicit default port
    // Make sure it uses the port from the hftp URI.
    fsUri = URI.create("hftp://localhost:"+httpPort);
    fs = (MyHftpFileSystem) FileSystem.newInstance(fsUri, conf);
    assertEquals(httpPort, fs.getCanonicalUri().getPort());
    checkTokenSelection(fs, httpPort, conf);

    // test with non-default port
    // Make sure it uses the port from the hftp URI.
    fsUri = URI.create("hftp://localhost:"+(httpPort+1));
    fs = (MyHftpFileSystem) FileSystem.newInstance(fsUri, conf);
    assertEquals(httpPort+1, fs.getCanonicalUri().getPort());
    checkTokenSelection(fs, httpPort + 1, conf);

    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY, 5);
  }

  @Test
  public void testSelectHsftpDelegationToken() throws Exception {
    SecurityUtilTestHelper.setTokenServiceUseIp(true);

    Configuration conf = new Configuration();
    conf.setClass("fs.hsftp.impl", MyHsftpFileSystem.class, FileSystem.class);

    int httpPort = 80;
    int httpsPort = 443;
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY, httpPort);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY, httpsPort);

    // test with implicit default port
    URI fsUri = URI.create("hsftp://localhost");
    MyHsftpFileSystem fs = (MyHsftpFileSystem) FileSystem.newInstance(fsUri, conf);
    assertEquals(httpsPort, fs.getCanonicalUri().getPort());
    checkTokenSelection(fs, httpsPort, conf);

    // test with explicit default port
    fsUri = URI.create("hsftp://localhost:"+httpsPort);
    fs = (MyHsftpFileSystem) FileSystem.newInstance(fsUri, conf);
    assertEquals(httpsPort, fs.getCanonicalUri().getPort());
    checkTokenSelection(fs, httpsPort, conf);

    // test with non-default port
    fsUri = URI.create("hsftp://localhost:"+(httpsPort+1));
    fs = (MyHsftpFileSystem) FileSystem.newInstance(fsUri, conf);
    assertEquals(httpsPort+1, fs.getCanonicalUri().getPort());
    checkTokenSelection(fs, httpsPort+1, conf);

    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY, 5);
  }


  @Test
  public void testInsecureRemoteCluster()  throws Exception {
    final ServerSocket socket = new ServerSocket(0); // just reserve a port
    socket.close();
    Configuration conf = new Configuration();
    URI fsUri = URI.create("hsftp://localhost:"+socket.getLocalPort());
    assertNull(FileSystem.newInstance(fsUri, conf).getDelegationToken(null));
  }

  @Test
  public void testSecureClusterError()  throws Exception {
    final ServerSocket socket = new ServerSocket(0);
    Thread t = new Thread() {
      @Override
      public void run() {
        while (true) { // fetching does a few retries
          try {
            Socket s = socket.accept();
            s.getOutputStream().write(1234);
            s.shutdownOutput();
          } catch (Exception e) {
            break;
          }
        }
      }
    };
    t.start();

    try {
      Configuration conf = new Configuration();
      URI fsUri = URI.create("hsftp://localhost:"+socket.getLocalPort());
      Exception ex = null;
      try {
        FileSystem.newInstance(fsUri, conf).getDelegationToken(null);
      } catch (Exception e) {
        ex = e;
      }
      assertNotNull(ex);
      assertNotNull(ex.getCause());
      assertEquals("Remote host closed connection during handshake",
                   ex.getCause().getMessage());
    } finally {
      t.interrupt();
    }
  }

  private void checkTokenSelection(HftpFileSystem fs,
                                   int port,
                                   Configuration conf) throws IOException {
    UserGroupInformation ugi =
        UserGroupInformation.createUserForTesting(fs.getUri().getAuthority(), new String[]{});

    // use ip-based tokens
    SecurityUtilTestHelper.setTokenServiceUseIp(true);

    // test fallback to hdfs token
    Token<?> hdfsToken = new Token<TokenIdentifier>(
        new byte[0], new byte[0],
        DelegationTokenIdentifier.HDFS_DELEGATION_KIND,
        new Text("127.0.0.1:8020"));
    ugi.addToken(hdfsToken);

    // test fallback to hdfs token
    Token<?> token = fs.selectDelegationToken(ugi);
    assertNotNull(token);
    assertEquals(hdfsToken, token);

    // test hftp is favored over hdfs
    Token<?> hftpToken = new Token<TokenIdentifier>(
        new byte[0], new byte[0],
        HftpFileSystem.TOKEN_KIND, new Text("127.0.0.1:"+port));
    ugi.addToken(hftpToken);
    token = fs.selectDelegationToken(ugi);
    assertNotNull(token);
    assertEquals(hftpToken, token);

    // switch to using host-based tokens, no token should match
    SecurityUtilTestHelper.setTokenServiceUseIp(false);
    token = fs.selectDelegationToken(ugi);
    assertNull(token);

    // test fallback to hdfs token
    hdfsToken = new Token<TokenIdentifier>(
        new byte[0], new byte[0],
        DelegationTokenIdentifier.HDFS_DELEGATION_KIND,
        new Text("localhost:8020"));
    ugi.addToken(hdfsToken);
    token = fs.selectDelegationToken(ugi);
    assertNotNull(token);
    assertEquals(hdfsToken, token);

    // test hftp is favored over hdfs
    hftpToken = new Token<TokenIdentifier>(
        new byte[0], new byte[0],
        HftpFileSystem.TOKEN_KIND, new Text("localhost:"+port));
    ugi.addToken(hftpToken);
    token = fs.selectDelegationToken(ugi);
    assertNotNull(token);
    assertEquals(hftpToken, token);
  }

  static class MyHftpFileSystem extends HftpFileSystem {
    @Override
    public URI getCanonicalUri() {
      return super.getCanonicalUri();
    }
    @Override
    public int getDefaultPort() {
      return super.getDefaultPort();
    }
    // don't automatically get a token
    @Override
    protected void initDelegationToken() throws IOException {}
  }

  static class MyHsftpFileSystem extends HsftpFileSystem {
    @Override
    public URI getCanonicalUri() {
      return super.getCanonicalUri();
    }
    @Override
    public int getDefaultPort() {
      return super.getDefaultPort();
    }
    // don't automatically get a token
    @Override
    protected void initDelegationToken() throws IOException {}
  }
}

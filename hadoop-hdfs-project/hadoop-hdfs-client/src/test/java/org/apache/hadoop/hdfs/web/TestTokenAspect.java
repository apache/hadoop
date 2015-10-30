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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegationTokenRenewer;
import org.apache.hadoop.fs.DelegationTokenRenewer.RenewAction;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

public class TestTokenAspect {

  private static class DummyFs extends FileSystem implements
      DelegationTokenRenewer.Renewable, TokenAspect.TokenManagementDelegator {

    private static final Text TOKEN_KIND = new Text("DummyFS Token");
    private boolean emulateSecurityEnabled;
    private TokenAspect<DummyFs> tokenAspect;
    private final UserGroupInformation ugi = UserGroupInformation
        .createUserForTesting("foo", new String[] { "bar" });
    private URI uri;

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
        Progressable progress) throws IOException {
      return null;
    }

    @Override
    public void cancelDelegationToken(Token<?> token) throws IOException {
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
        boolean overwrite, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      return null;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      return false;
    }

    @Override
    public URI getCanonicalUri() {
      return super.getCanonicalUri();
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      return null;
    }

    @Override
    public Token<?> getRenewToken() {
      return null;
    }

    @Override
    public URI getUri() {
      return uri;
    }

    @Override
    public Path getWorkingDirectory() {
      return null;
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
      super.initialize(name, conf);
      setConf(conf);
      this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());
      tokenAspect = new TokenAspect<DummyFs>(this,
          SecurityUtil.buildTokenService(uri), TOKEN_KIND);
      if (emulateSecurityEnabled || UserGroupInformation.isSecurityEnabled()) {
        tokenAspect.initDelegationToken(ugi);
      }
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException,
        IOException {
      return null;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      return false;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      return null;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      return false;
    }

    @Override
    public long renewDelegationToken(Token<?> token) throws IOException {
      return 0;
    }

    @Override
    public <T extends TokenIdentifier> void setDelegationToken(Token<T> token) {
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
    }
  }

  private static RenewAction<?> getActionFromTokenAspect(
      TokenAspect<DummyFs> tokenAspect) {
    return (RenewAction<?>) Whitebox.getInternalState(tokenAspect, "action");
  }

  @Test
  public void testCachedInitialization() throws IOException, URISyntaxException {
    Configuration conf = new Configuration();
    DummyFs fs = spy(new DummyFs());
    Token<TokenIdentifier> token = new Token<TokenIdentifier>(new byte[0],
        new byte[0], DummyFs.TOKEN_KIND, new Text("127.0.0.1:1234"));

    doReturn(token).when(fs).getDelegationToken(anyString());
    doReturn(token).when(fs).getRenewToken();

    fs.emulateSecurityEnabled = true;
    fs.initialize(new URI("dummyfs://127.0.0.1:1234"), conf);

    fs.tokenAspect.ensureTokenInitialized();
    verify(fs, times(1)).getDelegationToken(null);
    verify(fs, times(1)).setDelegationToken(token);

    // For the second iteration, the token should be cached.
    fs.tokenAspect.ensureTokenInitialized();
    verify(fs, times(1)).getDelegationToken(null);
    verify(fs, times(1)).setDelegationToken(token);
  }

  @Test
  public void testGetRemoteToken() throws IOException, URISyntaxException {
    Configuration conf = new Configuration();
    DummyFs fs = spy(new DummyFs());
    Token<TokenIdentifier> token = new Token<TokenIdentifier>(new byte[0],
        new byte[0], DummyFs.TOKEN_KIND, new Text("127.0.0.1:1234"));

    doReturn(token).when(fs).getDelegationToken(anyString());
    doReturn(token).when(fs).getRenewToken();

    fs.initialize(new URI("dummyfs://127.0.0.1:1234"), conf);

    fs.tokenAspect.ensureTokenInitialized();

    // Select a token, store and renew it
    verify(fs).setDelegationToken(token);
    assertNotNull(Whitebox.getInternalState(fs.tokenAspect, "dtRenewer"));
    assertNotNull(Whitebox.getInternalState(fs.tokenAspect, "action"));
  }

  @Test
  public void testGetRemoteTokenFailure() throws IOException,
      URISyntaxException {
    Configuration conf = new Configuration();
    DummyFs fs = spy(new DummyFs());
    IOException e = new IOException();
    doThrow(e).when(fs).getDelegationToken(anyString());

    fs.emulateSecurityEnabled = true;
    fs.initialize(new URI("dummyfs://127.0.0.1:1234"), conf);
    try {
      fs.tokenAspect.ensureTokenInitialized();
    } catch (IOException exc) {
      assertEquals(e, exc);
    }
  }

  @Test
  public void testInitWithNoTokens() throws IOException, URISyntaxException {
    Configuration conf = new Configuration();
    DummyFs fs = spy(new DummyFs());
    doReturn(null).when(fs).getDelegationToken(anyString());
    fs.initialize(new URI("dummyfs://127.0.0.1:1234"), conf);

    fs.tokenAspect.ensureTokenInitialized();

    // No token will be selected.
    verify(fs, never()).setDelegationToken(
        Mockito.<Token<? extends TokenIdentifier>> any());
  }

  @Test
  public void testInitWithUGIToken() throws IOException, URISyntaxException {
    Configuration conf = new Configuration();
    DummyFs fs = spy(new DummyFs());
    doReturn(null).when(fs).getDelegationToken(anyString());

    Token<TokenIdentifier> token = new Token<TokenIdentifier>(new byte[0],
        new byte[0], DummyFs.TOKEN_KIND, new Text("127.0.0.1:1234"));
    fs.ugi.addToken(token);
    fs.ugi.addToken(new Token<TokenIdentifier>(new byte[0], new byte[0],
        new Text("Other token"), new Text("127.0.0.1:8021")));
    assertEquals("wrong tokens in user", 2, fs.ugi.getTokens().size());

    fs.emulateSecurityEnabled = true;
    fs.initialize(new URI("dummyfs://127.0.0.1:1234"), conf);
    fs.tokenAspect.ensureTokenInitialized();

    // Select a token from ugi (not from the remote host), store it but don't
    // renew it
    verify(fs).setDelegationToken(token);
    verify(fs, never()).getDelegationToken(anyString());
    assertNull(Whitebox.getInternalState(fs.tokenAspect, "dtRenewer"));
    assertNull(Whitebox.getInternalState(fs.tokenAspect, "action"));
  }

  @Test
  public void testRenewal() throws Exception {
    Configuration conf = new Configuration();
    Token<?> token1 = mock(Token.class);
    Token<?> token2 = mock(Token.class);
    final long renewCycle = 100;
    DelegationTokenRenewer.renewCycle = renewCycle;

    UserGroupInformation ugi = UserGroupInformation.createUserForTesting("foo",
        new String[] { "bar" });
    DummyFs fs = spy(new DummyFs());

    doReturn(token1).doReturn(token2).when(fs).getDelegationToken(null);
    doReturn(token1).when(fs).getRenewToken();
    // cause token renewer to abandon the token
    doThrow(new IOException("renew failed")).when(token1).renew(conf);
    doThrow(new IOException("get failed")).when(fs).addDelegationTokens(null,
        null);

    final URI uri = new URI("dummyfs://127.0.0.1:1234");
    TokenAspect<DummyFs> tokenAspect = new TokenAspect<DummyFs>(fs,
        SecurityUtil.buildTokenService(uri), DummyFs.TOKEN_KIND);
    fs.initialize(uri, conf);
    tokenAspect.initDelegationToken(ugi);

    // trigger token acquisition
    tokenAspect.ensureTokenInitialized();
    DelegationTokenRenewer.RenewAction<?> action = getActionFromTokenAspect(tokenAspect);
    verify(fs).setDelegationToken(token1);
    assertTrue(action.isValid());

    // upon renewal, token will go bad based on above stubbing
    Thread.sleep(renewCycle * 2);
    assertSame(action, getActionFromTokenAspect(tokenAspect));
    assertFalse(action.isValid());

    // now that token is invalid, should get a new one
    tokenAspect.ensureTokenInitialized();
    verify(fs, times(2)).getDelegationToken(anyString());
    verify(fs).setDelegationToken(token2);
    assertNotSame(action, getActionFromTokenAspect(tokenAspect));

    action = getActionFromTokenAspect(tokenAspect);
    assertTrue(action.isValid());
  }
}

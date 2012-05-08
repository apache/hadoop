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

package org.apache.hadoop.mapreduce.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Master;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestTokenCache {

  @Test
  @SuppressWarnings("deprecation")
  public void testGetDelegationTokensNotImplemented() throws Exception {
    Credentials credentials = new Credentials();
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_PRINCIPAL, "mapred/host@REALM");
    String renewer = Master.getMasterPrincipal(conf);

    FileSystem fs = setupSingleFsWithoutGetDelegationTokens();
    TokenCache.obtainTokensForNamenodesInternal(fs, credentials, conf);
    assertEquals(1, credentials.getAllTokens().size());

    verify(fs).getDelegationTokens(renewer, credentials);
    verify(fs).getDelegationToken(renewer);
  }

  @Test
  public void testManagedFileSystem() throws Exception {
    Credentials credentials = new Credentials();
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_PRINCIPAL, "mapred/host@REALM");
    String renewer = Master.getMasterPrincipal(conf);

    FileSystem singleFs = setupSingleFs();
    FileSystem multiFs = setupMultiFs(singleFs, renewer, credentials);

    TokenCache.obtainTokensForNamenodesInternal(singleFs, credentials, conf);
    assertEquals(1, credentials.getAllTokens().size());

    TokenCache.obtainTokensForNamenodesInternal(singleFs, credentials, conf);
    assertEquals(1, credentials.getAllTokens().size());

    TokenCache.obtainTokensForNamenodesInternal(multiFs, credentials, conf);
    assertEquals(2, credentials.getAllTokens().size());

    TokenCache.obtainTokensForNamenodesInternal(multiFs, credentials, conf);
    assertEquals(2, credentials.getAllTokens().size());

    verify(singleFs, times(1)).getDelegationTokens(renewer, credentials);
    verify(multiFs, times(2)).getDelegationTokens(renewer, credentials);
    // A call to getDelegationToken would have generated an exception.
  }

  @SuppressWarnings("deprecation")
  private FileSystem setupSingleFsWithoutGetDelegationTokens() throws Exception {
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.getCanonicalServiceName()).thenReturn("singlefs4");
    when(mockFs.getUri()).thenReturn(new URI("singlefs4:///"));

    final Token<?> mockToken = (Token<?>) mock(Token.class);
    when(mockToken.getService()).thenReturn(new Text("singlefs4"));

    when(mockFs.getDelegationToken(any(String.class))).thenAnswer(
        new Answer<Token<?>>() {
          @Override
          public Token<?> answer(InvocationOnMock invocation) throws Throwable {
            return mockToken;
          }
        });

    when(mockFs.getDelegationTokens(any(String.class), any(Credentials.class)))
        .thenReturn(new LinkedList<Token<?>>());

    return mockFs;
  }

  private FileSystem setupSingleFs() throws Exception {
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.getCanonicalServiceName()).thenReturn("singlefs1");
    when(mockFs.getUri()).thenReturn(new URI("singlefs1:///"));

    List<Token<?>> tokens = new LinkedList<Token<?>>();
    Token<?> mockToken = mock(Token.class);
    when(mockToken.getService()).thenReturn(new Text("singlefs1"));
    tokens.add(mockToken);

    when(mockFs.getDelegationTokens(any(String.class))).thenThrow(
        new RuntimeException(
            "getDelegationTokens(renewer) should not be called"));
    when(mockFs.getDelegationTokens(any(String.class), any(Credentials.class)))
        .thenReturn(tokens);

    return mockFs;
  }

  private FileSystem setupMultiFs(final FileSystem singleFs,
      final String renewer, final Credentials credentials) throws Exception {
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.getCanonicalServiceName()).thenReturn(null);
    when(mockFs.getUri()).thenReturn(new URI("multifs:///"));

    when(mockFs.getDelegationTokens(any(String.class))).thenThrow(
        new RuntimeException(
            "getDelegationTokens(renewer) should not be called"));
    when(mockFs.getDelegationTokens(renewer, credentials)).thenAnswer(
        new Answer<List<Token<?>>>() {

          @Override
          public List<Token<?>> answer(InvocationOnMock invocation)
              throws Throwable {
            List<Token<?>> newTokens = new LinkedList<Token<?>>();
            if (credentials.getToken(new Text("singlefs1")) == null) {
              newTokens.addAll(singleFs.getDelegationTokens(renewer,
                  credentials));
            } else {
              newTokens.add(credentials.getToken(new Text("singlefs1")));
            }
            Token<?> mockToken2 = mock(Token.class);
            when(mockToken2.getService()).thenReturn(new Text("singlefs2"));
            newTokens.add(mockToken2);
            return newTokens;
          }
        });

    return mockFs;
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testBinaryCredentials() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_PRINCIPAL, "mapred/host@REALM");
    String renewer = Master.getMasterPrincipal(conf);

    Path TEST_ROOT_DIR =
        new Path(System.getProperty("test.build.data","test/build/data"));
    // ick, but need fq path minus file:/
    String binaryTokenFile = FileSystem.getLocal(conf).makeQualified(
        new Path(TEST_ROOT_DIR, "tokenFile")).toUri().getPath();

    FileSystem fs1 = createFileSystemForService("service1");
    FileSystem fs2 = createFileSystemForService("service2");
    FileSystem fs3 = createFileSystemForService("service3");
    
    // get the tokens for fs1 & fs2 and write out to binary creds file
    Credentials creds = new Credentials();
    Token<?> token1 = fs1.getDelegationToken(renewer);
    Token<?> token2 = fs2.getDelegationToken(renewer);
    creds.addToken(token1.getService(), token1);
    creds.addToken(token2.getService(), token2);
    // wait to set, else the obtain tokens call above will fail with FNF
    conf.set(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY, binaryTokenFile);
    creds.writeTokenStorageFile(new Path(binaryTokenFile), conf);
    
    // re-init creds and add a newer token for fs1
    creds = new Credentials();
    Token<?> newerToken1 = fs1.getDelegationToken(renewer);
    assertFalse(newerToken1.equals(token1));
    creds.addToken(newerToken1.getService(), newerToken1);
    checkToken(creds, newerToken1);
    
    // get token for fs1, see that fs2's token was loaded 
    TokenCache.obtainTokensForNamenodesInternal(fs1, creds, conf);
    checkToken(creds, newerToken1, token2);
    
    // get token for fs2, nothing should change since already present
    TokenCache.obtainTokensForNamenodesInternal(fs2, creds, conf);
    checkToken(creds, newerToken1, token2);
    
    // get token for fs3, should only add token for fs3
    TokenCache.obtainTokensForNamenodesInternal(fs3, creds, conf);
    Token<?> token3 = creds.getToken(new Text(fs3.getCanonicalServiceName()));
    assertTrue(token3 != null);
    checkToken(creds, newerToken1, token2, token3);
    
    // be paranoid, check one last time that nothing changes
    TokenCache.obtainTokensForNamenodesInternal(fs1, creds, conf);
    TokenCache.obtainTokensForNamenodesInternal(fs2, creds, conf);
    TokenCache.obtainTokensForNamenodesInternal(fs3, creds, conf);
    checkToken(creds, newerToken1, token2, token3);
  }

  private void checkToken(Credentials creds, Token<?> ... tokens) {
    assertEquals(tokens.length, creds.getAllTokens().size());
    for (Token<?> token : tokens) {
      Token<?> credsToken = creds.getToken(token.getService());
      assertTrue(credsToken != null);
      assertEquals(token, credsToken);
    }
  }
  
  @SuppressWarnings("deprecation")
  private FileSystem createFileSystemForService(final String service)
      throws IOException {
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.getCanonicalServiceName()).thenReturn(service);
    when(mockFs.getDelegationToken(any(String.class))).thenAnswer(
        new Answer<Token<?>>() {
          int unique = 0;
          @Override
          public Token<?> answer(InvocationOnMock invocation) throws Throwable {
            Token<?> token = new Token<TokenIdentifier>();
            token.setService(new Text(service));
            // use unique value so when we restore from token storage, we can
            // tell if it's really the same token
            token.setKind(new Text("token" + unique++));
            return token;
          }
        });
    return mockFs;
  }

  @Test
  public void testSingleTokenFetch() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_PRINCIPAL, "mapred/host@REALM");
    String renewer = Master.getMasterPrincipal(conf);
    Credentials credentials = new Credentials();
    
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.getCanonicalServiceName()).thenReturn("host:0");
    when(mockFs.getUri()).thenReturn(new URI("mockfs://host:0"));
    
    Path mockPath = mock(Path.class);
    when(mockPath.getFileSystem(conf)).thenReturn(mockFs);
    
    Path[] paths = new Path[]{ mockPath, mockPath };
    when(mockFs.getDelegationTokens("me", credentials)).thenReturn(null);
    TokenCache.obtainTokensForNamenodesInternal(credentials, paths, conf);
    verify(mockFs, times(1)).getDelegationTokens(renewer, credentials);
  }

  @Test
  public void testCleanUpTokenReferral() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY, "foo");
    TokenCache.cleanUpTokenReferral(conf);
    assertNull(conf.get(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY));
  }
  
}

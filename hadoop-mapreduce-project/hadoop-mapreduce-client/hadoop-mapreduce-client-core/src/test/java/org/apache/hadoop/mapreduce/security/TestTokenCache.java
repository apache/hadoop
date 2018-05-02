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

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Master;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestTokenCache {
  private static Configuration conf;
  private static String renewer;
  
  @BeforeClass
  public static void setup() throws Exception {
    conf = new Configuration();
    conf.set(YarnConfiguration.RM_PRINCIPAL, "mapred/host@REALM");
    renewer = Master.getMasterPrincipal(conf);
  }

  @Test
  public void testObtainTokens() throws Exception {
    Credentials credentials = new Credentials();
    FileSystem fs = mock(FileSystem.class);
    TokenCache.obtainTokensForNamenodesInternal(fs, credentials, conf, renewer);
    verify(fs).addDelegationTokens(eq(renewer), eq(credentials));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testBinaryCredentialsWithoutScheme() throws Exception {
    testBinaryCredentials(false);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testBinaryCredentialsWithScheme() throws Exception {
    testBinaryCredentials(true);
  }

  private void testBinaryCredentials(boolean hasScheme) throws Exception {
    Path TEST_ROOT_DIR =
        new Path(System.getProperty("test.build.data","test/build/data"));
    // ick, but need fq path minus file:/
    String binaryTokenFile = hasScheme
        ? FileSystem.getLocal(conf).makeQualified(
            new Path(TEST_ROOT_DIR, "tokenFile")).toString()
        : FileSystem.getLocal(conf).makeQualified(
            new Path(TEST_ROOT_DIR, "tokenFile")).toUri().getPath();

    MockFileSystem fs1 = createFileSystemForServiceName("service1");
    MockFileSystem fs2 = createFileSystemForServiceName("service2");
    MockFileSystem fs3 = createFileSystemForServiceName("service3");
    
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
    assertNotSame(newerToken1, token1);
    creds.addToken(newerToken1.getService(), newerToken1);
    checkToken(creds, newerToken1);
    
    // get token for fs1, see that fs2's token was loaded 
    TokenCache.obtainTokensForNamenodesInternal(fs1, creds, conf, renewer);
    checkToken(creds, newerToken1, token2);
    
    // get token for fs2, nothing should change since already present
    TokenCache.obtainTokensForNamenodesInternal(fs2, creds, conf, renewer);
    checkToken(creds, newerToken1, token2);
    
    // get token for fs3, should only add token for fs3
    TokenCache.obtainTokensForNamenodesInternal(fs3, creds, conf, renewer);
    Token<?> token3 = creds.getToken(new Text(fs3.getCanonicalServiceName()));
    assertTrue(token3 != null);
    checkToken(creds, newerToken1, token2, token3);
    
    // be paranoid, check one last time that nothing changes
    TokenCache.obtainTokensForNamenodesInternal(fs1, creds, conf, renewer);
    TokenCache.obtainTokensForNamenodesInternal(fs2, creds, conf, renewer);
    TokenCache.obtainTokensForNamenodesInternal(fs3, creds, conf, renewer);
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
  
  private MockFileSystem createFileSystemForServiceName(final String service)
      throws IOException {
    MockFileSystem mockFs = new MockFileSystem();
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
    
    final MockFileSystem fs = new MockFileSystem();
    final MockFileSystem mockFs = (MockFileSystem) fs.getRawFileSystem();
    when(mockFs.getCanonicalServiceName()).thenReturn("host:0");
    when(mockFs.getUri()).thenReturn(new URI("mockfs://host:0"));
    
    Path mockPath = mock(Path.class);
    when(mockPath.getFileSystem(conf)).thenReturn(mockFs);
    
    Path[] paths = new Path[]{ mockPath, mockPath };
    when(mockFs.addDelegationTokens("me", credentials)).thenReturn(null);
    TokenCache.obtainTokensForNamenodesInternal(credentials, paths, conf);
    verify(mockFs, times(1)).addDelegationTokens(renewer, credentials);
  }

  @Test
  public void testCleanUpTokenReferral() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY, "foo");
    TokenCache.cleanUpTokenReferral(conf);
    assertNull(conf.get(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY));
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testGetTokensForNamenodes() throws IOException,
      URISyntaxException {
    Path TEST_ROOT_DIR =
        new Path(System.getProperty("test.build.data", "test/build/data"));
    // ick, but need fq path minus file:/
    String binaryTokenFile =
        FileSystem.getLocal(conf)
          .makeQualified(new Path(TEST_ROOT_DIR, "tokenFile")).toUri()
          .getPath();

    MockFileSystem fs1 = createFileSystemForServiceName("service1");
    Credentials creds = new Credentials();
    Token<?> token1 = fs1.getDelegationToken(renewer);
    creds.addToken(token1.getService(), token1);
    // wait to set, else the obtain tokens call above will fail with FNF
    conf.set(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY, binaryTokenFile);
    creds.writeTokenStorageFile(new Path(binaryTokenFile), conf);
    TokenCache.obtainTokensForNamenodesInternal(fs1, creds, conf, renewer);
    String fs_addr = fs1.getCanonicalServiceName();
    Token<?> nnt = TokenCache.getDelegationToken(creds, fs_addr);
    assertNotNull("Token for nn is null", nnt);
  }
}
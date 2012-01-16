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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Master;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
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
}

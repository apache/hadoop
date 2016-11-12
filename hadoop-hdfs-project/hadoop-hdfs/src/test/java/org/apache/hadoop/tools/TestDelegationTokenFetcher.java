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
package org.apache.hadoop.tools;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.tools.DelegationTokenFetcher;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.Matchers.*;

public class TestDelegationTokenFetcher {
  private DistributedFileSystem dfs;
  private Configuration conf;
  private URI uri;
  private static final String SERVICE_VALUE = "localhost:2005";
  private static final String tokenFile = "file.dta";

  @Before 
  public void init() throws URISyntaxException, IOException {
    dfs = mock(DistributedFileSystem.class);
    conf = new Configuration();
    uri = new URI("hdfs://" + SERVICE_VALUE);
    FileSystemTestHelper.addFileSystemForTesting(uri, conf, dfs);
  }
  
  /**
   * Verify that when the DelegationTokenFetcher runs, it talks to the Namenode,
   * pulls out the correct user's token and successfully serializes it to disk.
   */
  @Test
  public void expectedTokenIsRetrievedFromDFS() throws Exception {
    final byte[] ident = new DelegationTokenIdentifier(new Text("owner"),
        new Text("renewer"), new Text("realuser")).getBytes();
    final byte[] pw = new byte[] { 42 };
    final Text service = new Text(uri.toString());

    // Create a token for the fetcher to fetch, wire NN to return it when asked
    // for this particular user.
    final Token<DelegationTokenIdentifier> t = 
      new Token<DelegationTokenIdentifier>(ident, pw, FakeRenewer.KIND, service);
    when(dfs.addDelegationTokens(eq((String) null), any(Credentials.class))).thenAnswer(
        new Answer<Token<?>[]>() {
          @Override
          public Token<?>[] answer(InvocationOnMock invocation) {
            Credentials creds = (Credentials)invocation.getArguments()[1];
            creds.addToken(service, t);
            return new Token<?>[]{t};
          }
        });
    when(dfs.getUri()).thenReturn(uri);
    FakeRenewer.reset();

    FileSystem fileSys = FileSystem.getLocal(conf);
    try {
      DelegationTokenFetcher.main(new String[] { "-fs", uri.toString(),
          tokenFile });
      Path p = new Path(fileSys.getWorkingDirectory(), tokenFile);
      Credentials creds = Credentials.readTokenStorageFile(p, conf);
      Iterator<Token<?>> itr = creds.getAllTokens().iterator();
      // make sure we got back exactly the 1 token we expected
      assertTrue(itr.hasNext());
      assertEquals(t, itr.next());
      assertTrue(!itr.hasNext());

      DelegationTokenFetcher.main(new String[] { "--print", tokenFile });
      DelegationTokenFetcher.main(new String[] { "--renew", tokenFile });
      assertEquals(t, FakeRenewer.lastRenewed);
      FakeRenewer.reset();

      DelegationTokenFetcher.main(new String[] { "--cancel", tokenFile });
      assertEquals(t, FakeRenewer.lastCanceled);
    } finally {
      fileSys.delete(new Path(tokenFile), true);
    }
  }

  @Test
  public void testDelegationTokenWithoutRenewer() throws Exception {
    conf.setBoolean(DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
        .build();
    FileSystem localFs = FileSystem.getLocal(conf);
    try {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      // Should be able to fetch token without renewer.
      uri = fs.getUri();
      DelegationTokenFetcher.main(new String[] { "-fs", uri.toString(),
          tokenFile });
      Path p = new Path(localFs.getWorkingDirectory(), tokenFile);
      Credentials creds = Credentials.readTokenStorageFile(p, conf);
      Iterator<Token<?>> itr = creds.getAllTokens().iterator();
      // make sure we got back exactly the 1 token we expected
      assertTrue(itr.hasNext());
      final Token token = itr.next();
      assertNotNull("Token without renewer shouldn't be null", token);
      assertTrue(!itr.hasNext());
      try {
        // Without renewer renewal of token should fail.
        DelegationTokenFetcher.main(new String[] { "--renew", tokenFile });
        fail("Should have failed to renew");
      } catch (AccessControlException e) {
        GenericTestUtils.assertExceptionContains("tried to renew a token ("
            + token.decodeIdentifier() + ") without a renewer", e);
      }
    } finally {
      cluster.shutdown();
      localFs.delete(new Path(tokenFile), true);
    }
  }
}

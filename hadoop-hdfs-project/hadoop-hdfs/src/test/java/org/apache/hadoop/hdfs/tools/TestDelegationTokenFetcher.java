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

package org.apache.hadoop.hdfs.tools;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.tools.FakeRenewer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDelegationTokenFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestDelegationTokenFetcher.class);

  private Configuration conf = new Configuration();

  @Rule
  public TemporaryFolder f = new TemporaryFolder();
  private static final String tokenFile = "token";

  /**
   * try to fetch token without http server with IOException
   */
  @Test(expected = IOException.class)
  public void testTokenFetchFail() throws Exception {
    WebHdfsFileSystem fs = mock(WebHdfsFileSystem.class);
    doThrow(new IOException()).when(fs).getDelegationToken(any());
    Path p = new Path(f.getRoot().getAbsolutePath(), tokenFile);
    DelegationTokenFetcher.saveDelegationToken(conf, fs, null, p);
  }

  /**
   * Call fetch token using http server
   */
  @Test
  public void expectedTokenIsRetrievedFromHttp() throws Exception {
    final Token<DelegationTokenIdentifier> testToken = new Token<DelegationTokenIdentifier>(
        "id".getBytes(), "pwd".getBytes(), FakeRenewer.KIND, new Text(
            "127.0.0.1:1234"));

    WebHdfsFileSystem fs = mock(WebHdfsFileSystem.class);

    doReturn(testToken).when(fs).getDelegationToken(any());
    Path p = new Path(f.getRoot().getAbsolutePath(), tokenFile);
    DelegationTokenFetcher.saveDelegationToken(conf, fs, null, p);

    Credentials creds = Credentials.readTokenStorageFile(p, conf);
    Iterator<Token<?>> itr = creds.getAllTokens().iterator();
    assertTrue("token not exist error", itr.hasNext());

    Token<?> fetchedToken = itr.next();
    Assert.assertArrayEquals("token wrong identifier error",
        testToken.getIdentifier(), fetchedToken.getIdentifier());
    Assert.assertArrayEquals("token wrong password error",
        testToken.getPassword(), fetchedToken.getPassword());

    DelegationTokenFetcher.renewTokens(conf, p);
    Assert.assertEquals(testToken, FakeRenewer.getLastRenewed());

    DelegationTokenFetcher.cancelTokens(conf, p);
    Assert.assertEquals(testToken, FakeRenewer.getLastCanceled());
  }

  /**
   * If token returned is null, saveDelegationToken should not
   * throw nullPointerException
   */
  @Test
  public void testReturnedTokenIsNull() throws Exception {
    WebHdfsFileSystem fs = mock(WebHdfsFileSystem.class);
    doReturn(null).when(fs).getDelegationToken(anyString());
    Path p = new Path(f.getRoot().getAbsolutePath(), tokenFile);
    DelegationTokenFetcher.saveDelegationToken(conf, fs, null, p);
    // When Token returned is null, TokenFile should not exist
    Assert.assertFalse(p.getFileSystem(conf).exists(p));

  }

  @Test
  public void testDelegationTokenWithoutRenewerViaRPC() throws Exception {
    conf.setBoolean(DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
        .build();
    try {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      // Should be able to fetch token without renewer.
      LocalFileSystem localFileSystem = FileSystem.getLocal(conf);
      Path p = new Path(f.getRoot().getAbsolutePath(), tokenFile);
      p = localFileSystem.makeQualified(p);
      DelegationTokenFetcher.saveDelegationToken(conf, fs, null, p);
      Credentials creds = Credentials.readTokenStorageFile(p, conf);
      Iterator<Token<?>> itr = creds.getAllTokens().iterator();
      assertTrue("token not exist error", itr.hasNext());
      final Token token = itr.next();
      assertNotNull("Token should be there without renewer", token);

      // Test compatibility of DelegationTokenFetcher.printTokensToString
      String expectedNonVerbose = "Token (HDFS_DELEGATION_TOKEN token 1 for " +
          System.getProperty("user.name") + " with renewer ) for";
      String resNonVerbose =
          DelegationTokenFetcher.printTokensToString(conf, p, false);
      assertTrue("The non verbose output is expected to start with \""
          + expectedNonVerbose +"\"",
          resNonVerbose.startsWith(expectedNonVerbose));
      LOG.info(resNonVerbose);
      LOG.info(
          DelegationTokenFetcher.printTokensToString(conf, p, true));

      try {
        // Without renewer renewal of token should fail.
        DelegationTokenFetcher.renewTokens(conf, p);
        fail("Should have failed to renew");
      } catch (AccessControlException e) {
        GenericTestUtils.assertExceptionContains("tried to renew a token ("
            + token.decodeIdentifier() + ") without a renewer", e);
      }
    } finally {
      cluster.shutdown();
    }
  }
}

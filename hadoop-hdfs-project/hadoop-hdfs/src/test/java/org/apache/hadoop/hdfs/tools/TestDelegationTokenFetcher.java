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

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.tools.FakeRenewer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestDelegationTokenFetcher {

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
    doThrow(new IOException()).when(fs).getDelegationToken(anyString());
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

    doReturn(testToken).when(fs).getDelegationToken(anyString());
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
}

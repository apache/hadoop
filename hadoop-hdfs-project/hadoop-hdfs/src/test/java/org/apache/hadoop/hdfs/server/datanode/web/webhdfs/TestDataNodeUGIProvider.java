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

package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.mockito.Mockito.mock;
import io.netty.handler.codec.http.QueryStringDecoder;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.LengthParam;
import org.apache.hadoop.hdfs.web.resources.NamenodeAddressParam;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

public class TestDataNodeUGIProvider {
  private final URI uri = URI.create(WebHdfsConstants.WEBHDFS_SCHEME + "://"
      + "127.0.0.1:0");
  private final String PATH = "/foo";
  private final int OFFSET = 42;
  private final int LENGTH = 512;
  private final static int EXPIRE_AFTER_ACCESS = 5*1000;
  private Configuration conf;
  @Before
  public void setUp(){
    conf = WebHdfsTestUtil.createConf();
    conf.setInt(DFSConfigKeys.DFS_WEBHDFS_UGI_EXPIRE_AFTER_ACCESS_KEY,
        EXPIRE_AFTER_ACCESS);
    DataNodeUGIProvider.init(conf);
  }

  @Test
  public void testUGICacheSecure() throws Exception {
    // fake turning on security so api thinks it should use tokens
    SecurityUtil.setAuthenticationMethod(KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);

    UserGroupInformation ugi = UserGroupInformation
        .createRemoteUser("test-user");
    ugi.setAuthenticationMethod(KERBEROS);
    ugi = UserGroupInformation.createProxyUser("test-proxy-user", ugi);
    UserGroupInformation.setLoginUser(ugi);

    List<Token<DelegationTokenIdentifier>> tokens = Lists.newArrayList();
    getWebHdfsFileSystem(ugi, conf, tokens);

    String uri1 = WebHdfsFileSystem.PATH_PREFIX
        + PATH
        + "?op=OPEN"
        + Param.toSortedString("&", new NamenodeAddressParam("127.0.0.1:1010"),
            new OffsetParam((long) OFFSET), new LengthParam((long) LENGTH),
            new DelegationParam(tokens.get(0).encodeToUrlString()));

    String uri2 = WebHdfsFileSystem.PATH_PREFIX
        + PATH
        + "?op=OPEN"
        + Param.toSortedString("&", new NamenodeAddressParam("127.0.0.1:1010"),
            new OffsetParam((long) OFFSET), new LengthParam((long) LENGTH),
            new DelegationParam(tokens.get(1).encodeToUrlString()));

    DataNodeUGIProvider ugiProvider1 = new DataNodeUGIProvider(
        new ParameterParser(new QueryStringDecoder(URI.create(uri1)), conf));
    UserGroupInformation ugi11 = ugiProvider1.ugi();
    UserGroupInformation ugi12 = ugiProvider1.ugi();

    Assert.assertEquals(
        "With UGI cache, two UGIs returned by the same token should be same",
        ugi11, ugi12);

    DataNodeUGIProvider ugiProvider2 = new DataNodeUGIProvider(
        new ParameterParser(new QueryStringDecoder(URI.create(uri2)), conf));
    UserGroupInformation url21 = ugiProvider2.ugi();
    UserGroupInformation url22 = ugiProvider2.ugi();

    Assert.assertEquals(
        "With UGI cache, two UGIs returned by the same token should be same",
        url21, url22);

    Assert.assertNotEquals(
        "With UGI cache, two UGIs for the different token should not be same",
        ugi11, url22);

    ugiProvider2.clearCache();
    awaitCacheEmptyDueToExpiration();
    ugi12 = ugiProvider1.ugi();
    url22 = ugiProvider2.ugi();

    String msg = "With cache eviction, two UGIs returned" +
    " by the same token should not be same";
    Assert.assertNotEquals(msg, ugi11, ugi12);
    Assert.assertNotEquals(msg, url21, url22);

    Assert.assertNotEquals(
        "With UGI cache, two UGIs for the different token should not be same",
        ugi11, url22);
  }

  @Test
  public void testUGICacheInSecure() throws Exception {
    String uri1 = WebHdfsFileSystem.PATH_PREFIX
        + PATH
        + "?op=OPEN"
        + Param.toSortedString("&", new OffsetParam((long) OFFSET),
            new LengthParam((long) LENGTH), new UserParam("root"));

    String uri2 = WebHdfsFileSystem.PATH_PREFIX
        + PATH
        + "?op=OPEN"
        + Param.toSortedString("&", new OffsetParam((long) OFFSET),
            new LengthParam((long) LENGTH), new UserParam("hdfs"));

    DataNodeUGIProvider ugiProvider1 = new DataNodeUGIProvider(
        new ParameterParser(new QueryStringDecoder(URI.create(uri1)), conf));
    UserGroupInformation ugi11 = ugiProvider1.ugi();
    UserGroupInformation ugi12 = ugiProvider1.ugi();

    Assert.assertEquals(
        "With UGI cache, two UGIs for the same user should be same", ugi11,
        ugi12);

    DataNodeUGIProvider ugiProvider2 = new DataNodeUGIProvider(
        new ParameterParser(new QueryStringDecoder(URI.create(uri2)), conf));
    UserGroupInformation url21 = ugiProvider2.ugi();
    UserGroupInformation url22 = ugiProvider2.ugi();

    Assert.assertEquals(
        "With UGI cache, two UGIs for the same user should be same", url21,
        url22);

    Assert.assertNotEquals(
        "With UGI cache, two UGIs for the different user should not be same",
        ugi11, url22);

    awaitCacheEmptyDueToExpiration();
    ugi12 = ugiProvider1.ugi();
    url22 = ugiProvider2.ugi();

    String msg = "With cache eviction, two UGIs returned by" +
    " the same user should not be same";
    Assert.assertNotEquals(msg, ugi11, ugi12);
    Assert.assertNotEquals(msg, url21, url22);

    Assert.assertNotEquals(
        "With UGI cache, two UGIs for the different user should not be same",
        ugi11, url22);
  }

  /**
   * Wait for expiration of entries from the UGI cache.  We need to be careful
   * not to touch the entries in the cache while we're waiting for expiration.
   * If we did, then that would reset the clock on expiration for those entries.
   * Instead, we trigger internal clean-up of the cache and check for size 0.
   *
   * @throws Exception if there is any error
   */
  private void awaitCacheEmptyDueToExpiration() throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          DataNodeUGIProvider.ugiCache.cleanUp();
          return DataNodeUGIProvider.ugiCache.size() == 0;
        }
      }, EXPIRE_AFTER_ACCESS, 10 * EXPIRE_AFTER_ACCESS);
  }

  private WebHdfsFileSystem getWebHdfsFileSystem(UserGroupInformation ugi,
      Configuration conf, List<Token<DelegationTokenIdentifier>> tokens)
      throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(new Text(
          ugi.getUserName()), null, null);
      FSNamesystem namesystem = mock(FSNamesystem.class);
      DelegationTokenSecretManager dtSecretManager = new DelegationTokenSecretManager(
          86400000, 86400000, 86400000, 86400000, namesystem);
      dtSecretManager.startThreads();
      Token<DelegationTokenIdentifier> token1 = new Token<DelegationTokenIdentifier>(
          dtId, dtSecretManager);
      Token<DelegationTokenIdentifier> token2 = new Token<DelegationTokenIdentifier>(
          dtId, dtSecretManager);
      SecurityUtil.setTokenService(token1,
          NetUtils.createSocketAddr(uri.getAuthority()));
      SecurityUtil.setTokenService(token2,
          NetUtils.createSocketAddr(uri.getAuthority()));
      token1.setKind(WebHdfsConstants.WEBHDFS_TOKEN_KIND);
      token2.setKind(WebHdfsConstants.WEBHDFS_TOKEN_KIND);

      tokens.add(token1);
      tokens.add(token2);

      ugi.addToken(token1);
      ugi.addToken(token2);
    }
    return (WebHdfsFileSystem) FileSystem.get(uri, conf);
  }
}

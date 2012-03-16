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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Joiner;

/**
 * Test case for client support of delegation tokens in an HA cluster.
 * See HDFS-2904 for more info.
 **/
public class TestDelegationTokensWithHA {
  private static Configuration conf = new Configuration();
  private static final Log LOG =
    LogFactory.getLog(TestDelegationTokensWithHA.class);
  private static MiniDFSCluster cluster;
  private static NameNode nn0;
  private static NameNode nn1;
  private static FileSystem fs;
  private static DelegationTokenSecretManager dtSecretManager;
  private static DistributedFileSystem dfs;

  @BeforeClass
  public static void setupCluster() throws Exception {
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    conf.set("hadoop.security.auth_to_local",
        "RULE:[2:$1@$0](JobTracker@.*FOO.COM)s/@.*//" + "DEFAULT");

    cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(0)
      .build();
    cluster.waitActive();
    
    nn0 = cluster.getNameNode(0);
    nn1 = cluster.getNameNode(1);
    fs = HATestUtil.configureFailoverFs(cluster, conf);
    dfs = (DistributedFileSystem)fs;

    cluster.transitionToActive(0);
    dtSecretManager = NameNodeAdapter.getDtSecretManager(
        nn0.getNamesystem());
  }

  @AfterClass
  public static void shutdownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }


  @Test
  public void testDelegationTokenDFSApi() throws Exception {
    Token<DelegationTokenIdentifier> token = dfs.getDelegationToken("JobTracker");
    DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
    byte[] tokenId = token.getIdentifier();
    identifier.readFields(new DataInputStream(
             new ByteArrayInputStream(tokenId)));

    // Ensure that it's present in the NN's secret manager and can
    // be renewed directly from there.
    LOG.info("A valid token should have non-null password, " +
        "and should be renewed successfully");
    assertTrue(null != dtSecretManager.retrievePassword(identifier));
    dtSecretManager.renewToken(token, "JobTracker");
    
    // Use the client conf with the failover info present to check
    // renewal.
    Configuration clientConf = dfs.getConf();
    doRenewOrCancel(token, clientConf, TokenTestAction.RENEW);
    
    // Using a configuration that doesn't have the logical nameservice
    // configured should result in a reasonable error message.
    Configuration emptyConf = new Configuration();
    try {
      doRenewOrCancel(token, emptyConf, TokenTestAction.RENEW);
      fail("Did not throw trying to renew with an empty conf!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "Unable to map logical nameservice URI", ioe);
    }

    
    // Ensure that the token can be renewed again after a failover.
    cluster.transitionToStandby(0);
    cluster.transitionToActive(1);
    doRenewOrCancel(token, clientConf, TokenTestAction.RENEW);
    
    doRenewOrCancel(token, clientConf, TokenTestAction.CANCEL);
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testDelegationTokenWithDoAs() throws Exception {
    final Token<DelegationTokenIdentifier> token = 
        dfs.getDelegationToken("JobTracker");
    final UserGroupInformation longUgi = UserGroupInformation
        .createRemoteUser("JobTracker/foo.com@FOO.COM");
    final UserGroupInformation shortUgi = UserGroupInformation
        .createRemoteUser("JobTracker");
    longUgi.doAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        DistributedFileSystem dfs = (DistributedFileSystem)
            HATestUtil.configureFailoverFs(cluster, conf);
        // try renew with long name
        dfs.renewDelegationToken(token);
        return null;
      }
    });
    shortUgi.doAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        DistributedFileSystem dfs = (DistributedFileSystem)
            HATestUtil.configureFailoverFs(cluster, conf);
        dfs.renewDelegationToken(token);
        return null;
      }
    });
    longUgi.doAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        DistributedFileSystem dfs = (DistributedFileSystem)
            HATestUtil.configureFailoverFs(cluster, conf);
        // try cancel with long name
        dfs.cancelDelegationToken(token);
        return null;
      }
    });
  }
  
  @Test
  public void testHAUtilClonesDelegationTokens() throws Exception {
    final Token<DelegationTokenIdentifier> token = 
      dfs.getDelegationToken("test");

    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("test");
    
    URI haUri = new URI("hdfs://my-ha-uri/");
    token.setService(HAUtil.buildTokenServiceForLogicalUri(haUri));
    ugi.addToken(token);
    HAUtil.cloneDelegationTokenForLogicalUri(ugi, haUri, nn0.getNameNodeAddress());
    HAUtil.cloneDelegationTokenForLogicalUri(ugi, haUri, nn1.getNameNodeAddress());
    
    Collection<Token<? extends TokenIdentifier>> tokens = ugi.getTokens();
    assertEquals(3, tokens.size());
    
    LOG.info("Tokens:\n" + Joiner.on("\n").join(tokens));
    
    // check that the token selected for one of the physical IPC addresses
    // matches the one we received
    InetSocketAddress addr = nn0.getNameNodeAddress();
    Text ipcDtService = new Text(
        addr.getAddress().getHostAddress() + ":" + addr.getPort());
    Token<DelegationTokenIdentifier> token2 =
        DelegationTokenSelector.selectHdfsDelegationToken(ipcDtService, ugi);
    assertNotNull(token2);
    assertArrayEquals(token.getIdentifier(), token2.getIdentifier());
    assertArrayEquals(token.getPassword(), token2.getPassword());
  }

  /**
   * HDFS-3062: DistributedFileSystem.getCanonicalServiceName() throws an
   * exception if the URI is a logical URI. This bug fails the combination of
   * ha + mapred + security.
   */
  @Test
  public void testDFSGetCanonicalServiceName() throws Exception {
    assertEquals(fs.getCanonicalServiceName(), 
        HATestUtil.getLogicalUri(cluster).getHost());
  }
  
  enum TokenTestAction {
    RENEW, CANCEL;
  }
  
  private static void doRenewOrCancel(
      final Token<DelegationTokenIdentifier> token, final Configuration conf,
      final TokenTestAction action)
      throws IOException, InterruptedException {
    UserGroupInformation.createRemoteUser("JobTracker").doAs(
        new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            switch (action) {
            case RENEW:
              token.renew(conf);
              break;
            case CANCEL:
              token.cancel(conf);
              break;
            default:
              fail("bad action:" + action);
            }
            return null;
          }
        });
  }
}

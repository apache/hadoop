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

import com.google.common.base.Joiner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.resources.ExceptionHandler;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.SecurityUtilTestHelper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import org.mortbay.util.ajax.JSON;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Test case for client support of delegation tokens in an HA cluster.
 * See HDFS-2904 for more info.
 **/
public class TestDelegationTokensWithHA {
  private static final Configuration conf = new Configuration();
  private static final Log LOG =
    LogFactory.getLog(TestDelegationTokensWithHA.class);
  private static MiniDFSCluster cluster;
  private static NameNode nn0;
  private static NameNode nn1;
  private static FileSystem fs;
  private static DelegationTokenSecretManager dtSecretManager;
  private static DistributedFileSystem dfs;

  private volatile boolean catchup = false;
  
  @Before
  public void setupCluster() throws Exception {
    SecurityUtilTestHelper.setTokenServiceUseIp(true);
    
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL,
        "RULE:[2:$1@$0](JobTracker@.*FOO.COM)s/@.*//" + "DEFAULT");

    cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(0)
      .build();
    cluster.waitActive();
    
    String logicalName = HATestUtil.getLogicalHostname(cluster);
    HATestUtil.setFailoverConfigurations(cluster, conf, logicalName, 0);

    nn0 = cluster.getNameNode(0);
    nn1 = cluster.getNameNode(1);
    fs = HATestUtil.configureFailoverFs(cluster, conf);
    dfs = (DistributedFileSystem)fs;

    cluster.transitionToActive(0);
    dtSecretManager = NameNodeAdapter.getDtSecretManager(
        nn0.getNamesystem());
  }

  @After
  public void shutdownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  public void testDelegationTokenDFSApi() throws Exception {
    final Token<DelegationTokenIdentifier> token =
        getDelegationToken(fs, "JobTracker");
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
  
  private class EditLogTailerForTest extends EditLogTailer {
    public EditLogTailerForTest(FSNamesystem namesystem, Configuration conf) {
      super(namesystem, conf);
    }
    
    public void catchupDuringFailover() throws IOException {
      synchronized (TestDelegationTokensWithHA.this) {
        while (!catchup) {
          try {
            LOG.info("The editlog tailer is waiting to catchup...");
            TestDelegationTokensWithHA.this.wait();
          } catch (InterruptedException e) {}
        }
      }
      super.catchupDuringFailover();
    }
  }
  
  /**
   * Test if correct exception (StandbyException or RetriableException) can be
   * thrown during the NN failover. 
   */
  @Test(timeout = 300000)
  public void testDelegationTokenDuringNNFailover() throws Exception {
    EditLogTailer editLogTailer = nn1.getNamesystem().getEditLogTailer();
    // stop the editLogTailer of nn1
    editLogTailer.stop();
    Configuration conf = (Configuration) Whitebox.getInternalState(
        editLogTailer, "conf");
    nn1.getNamesystem().setEditLogTailerForTests(
        new EditLogTailerForTest(nn1.getNamesystem(), conf));
    
    // create token
    final Token<DelegationTokenIdentifier> token =
        getDelegationToken(fs, "JobTracker");
    DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
    byte[] tokenId = token.getIdentifier();
    identifier.readFields(new DataInputStream(
             new ByteArrayInputStream(tokenId)));

    // Ensure that it's present in the nn0 secret manager and can
    // be renewed directly from there.
    LOG.info("A valid token should have non-null password, " +
        "and should be renewed successfully");
    assertTrue(null != dtSecretManager.retrievePassword(identifier));
    dtSecretManager.renewToken(token, "JobTracker");
    
    // transition nn0 to standby
    cluster.transitionToStandby(0);
    
    try {
      cluster.getNameNodeRpc(0).renewDelegationToken(token);
      fail("StandbyException is expected since nn0 is in standby state");
    } catch (StandbyException e) {
      GenericTestUtils.assertExceptionContains(
          HAServiceState.STANDBY.toString(), e);
    }
    
    new Thread() {
      @Override
      public void run() {
        try {
          cluster.transitionToActive(1);
        } catch (Exception e) {
          LOG.error("Transition nn1 to active failed", e);
        }    
      }
    }.start();
    
    Thread.sleep(1000);
    try {
      nn1.getNamesystem().verifyToken(token.decodeIdentifier(),
          token.getPassword());
      fail("RetriableException/StandbyException is expected since nn1 is in transition");
    } catch (IOException e) {
      assertTrue(e instanceof StandbyException
          || e instanceof RetriableException);
      LOG.info("Got expected exception", e);
    }
    
    catchup = true;
    synchronized (this) {
      this.notifyAll();
    }
    
    Configuration clientConf = dfs.getConf();
    doRenewOrCancel(token, clientConf, TokenTestAction.RENEW);
    doRenewOrCancel(token, clientConf, TokenTestAction.CANCEL);
  }

  @Test(timeout = 300000)
  public void testDelegationTokenWithDoAs() throws Exception {
    final Token<DelegationTokenIdentifier> token =
        getDelegationToken(fs, "JobTracker");
    final UserGroupInformation longUgi = UserGroupInformation
        .createRemoteUser("JobTracker/foo.com@FOO.COM");
    final UserGroupInformation shortUgi = UserGroupInformation
        .createRemoteUser("JobTracker");
    longUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        // try renew with long name
        token.renew(conf);
        return null;
      }
    });
    shortUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        token.renew(conf);
        return null;
      }
    });
    longUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        token.cancel(conf);;
        return null;
      }
    });
  }

  @Test(timeout = 300000)
  public void testHAUtilClonesDelegationTokens() throws Exception {
    final Token<DelegationTokenIdentifier> token =
        getDelegationToken(fs, "JobTracker");

    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("test");
    
    URI haUri = new URI("hdfs://my-ha-uri/");
    token.setService(HAUtil.buildTokenServiceForLogicalUri(haUri,
        HdfsConstants.HDFS_URI_SCHEME));
    ugi.addToken(token);

    Collection<InetSocketAddress> nnAddrs = new HashSet<InetSocketAddress>();
    nnAddrs.add(new InetSocketAddress("localhost",
      nn0.getNameNodeAddress().getPort()));
    nnAddrs.add(new InetSocketAddress("localhost",
      nn1.getNameNodeAddress().getPort()));
    HAUtil.cloneDelegationTokenForLogicalUri(ugi, haUri, nnAddrs);
    
    Collection<Token<? extends TokenIdentifier>> tokens = ugi.getTokens();
    assertEquals(3, tokens.size());
    
    LOG.info("Tokens:\n" + Joiner.on("\n").join(tokens));
    DelegationTokenSelector dts = new DelegationTokenSelector();
    
    // check that the token selected for one of the physical IPC addresses
    // matches the one we received
    for (InetSocketAddress addr : nnAddrs) {
      Text ipcDtService = SecurityUtil.buildTokenService(addr);
      Token<DelegationTokenIdentifier> token2 =
          dts.selectToken(ipcDtService, ugi.getTokens());
      assertNotNull(token2);
      assertArrayEquals(token.getIdentifier(), token2.getIdentifier());
      assertArrayEquals(token.getPassword(), token2.getPassword());
    }
    
    // switch to host-based tokens, shouldn't match existing tokens 
    SecurityUtilTestHelper.setTokenServiceUseIp(false);
    for (InetSocketAddress addr : nnAddrs) {
      Text ipcDtService = SecurityUtil.buildTokenService(addr);
      Token<DelegationTokenIdentifier> token2 =
          dts.selectToken(ipcDtService, ugi.getTokens());
      assertNull(token2);
    }
    
    // reclone the tokens, and see if they match now
    HAUtil.cloneDelegationTokenForLogicalUri(ugi, haUri, nnAddrs);
    for (InetSocketAddress addr : nnAddrs) {
      Text ipcDtService = SecurityUtil.buildTokenService(addr);
      Token<DelegationTokenIdentifier> token2 =
          dts.selectToken(ipcDtService, ugi.getTokens());
      assertNotNull(token2);
      assertArrayEquals(token.getIdentifier(), token2.getIdentifier());
      assertArrayEquals(token.getPassword(), token2.getPassword());
    }    
  }

  /**
   * HDFS-3062: DistributedFileSystem.getCanonicalServiceName() throws an
   * exception if the URI is a logical URI. This bug fails the combination of
   * ha + mapred + security.
   */
  @Test(timeout = 300000)
  public void testDFSGetCanonicalServiceName() throws Exception {
    URI hAUri = HATestUtil.getLogicalUri(cluster);
    String haService = HAUtil.buildTokenServiceForLogicalUri(hAUri,
        HdfsConstants.HDFS_URI_SCHEME).toString();
    assertEquals(haService, dfs.getCanonicalServiceName());
    final String renewer = UserGroupInformation.getCurrentUser().getShortUserName();
    final Token<DelegationTokenIdentifier> token =
        getDelegationToken(dfs, renewer);
    assertEquals(haService, token.getService().toString());
    // make sure the logical uri is handled correctly
    token.renew(dfs.getConf());
    token.cancel(dfs.getConf());
  }

  @Test(timeout = 300000)
  public void testHdfsGetCanonicalServiceName() throws Exception {
    Configuration conf = dfs.getConf();
    URI haUri = HATestUtil.getLogicalUri(cluster);
    AbstractFileSystem afs =  AbstractFileSystem.createFileSystem(haUri, conf);    
    String haService = HAUtil.buildTokenServiceForLogicalUri(haUri,
        HdfsConstants.HDFS_URI_SCHEME).toString();
    assertEquals(haService, afs.getCanonicalServiceName());
    Token<?> token = afs.getDelegationTokens(
        UserGroupInformation.getCurrentUser().getShortUserName()).get(0);
    assertEquals(haService, token.getService().toString());
    // make sure the logical uri is handled correctly
    token.renew(conf);
    token.cancel(conf);
  }
  
  /**
   * Test if StandbyException can be thrown from StandbyNN, when it's requested for 
   * password. (HDFS-6475). With StandbyException, the client can failover to try
   * activeNN.
   */
  @Test(timeout = 300000)
  public void testDelegationTokenStandbyNNAppearFirst() throws Exception {
    // make nn0 the standby NN, and nn1 the active NN
    cluster.transitionToStandby(0);
    cluster.transitionToActive(1);

    final DelegationTokenSecretManager stSecretManager = 
        NameNodeAdapter.getDtSecretManager(
            nn1.getNamesystem());

    // create token
    final Token<DelegationTokenIdentifier> token =
        getDelegationToken(fs, "JobTracker");
    final DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
    byte[] tokenId = token.getIdentifier();
    identifier.readFields(new DataInputStream(
             new ByteArrayInputStream(tokenId)));

    assertTrue(null != stSecretManager.retrievePassword(identifier));

    final UserGroupInformation ugi = UserGroupInformation
        .createRemoteUser("JobTracker");
    ugi.addToken(token);
    
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() {
        try {
          try {
            byte[] tmppw = dtSecretManager.retrievePassword(identifier);
            fail("InvalidToken with cause StandbyException is expected"
                + " since nn0 is standby");
            return tmppw;
          } catch (IOException e) {
            // Mimic the UserProvider class logic (server side) by throwing
            // SecurityException here
            throw new SecurityException(
                SecurityUtil.FAILED_TO_GET_UGI_MSG_HEADER + " " + e, e);
          }
        } catch (Exception oe) {
          //
          // The exception oe caught here is
          //     java.lang.SecurityException: Failed to obtain user group
          //     information: org.apache.hadoop.security.token.
          //     SecretManager$InvalidToken: StandbyException
          //
          HttpServletResponse response = mock(HttpServletResponse.class);
          ExceptionHandler eh = new ExceptionHandler();
          eh.initResponse(response);
          
          // The Response (resp) below is what the server will send to client          
          //
          // BEFORE HDFS-6475 fix, the resp.entity is
          //     {"RemoteException":{"exception":"SecurityException",
          //      "javaClassName":"java.lang.SecurityException",
          //      "message":"Failed to obtain user group information: 
          //      org.apache.hadoop.security.token.SecretManager$InvalidToken:
          //        StandbyException"}}
          // AFTER the fix, the resp.entity is
          //     {"RemoteException":{"exception":"StandbyException",
          //      "javaClassName":"org.apache.hadoop.ipc.StandbyException",
          //      "message":"Operation category READ is not supported in
          //       state standby"}}
          //
          Response resp = eh.toResponse(oe);
          
          // Mimic the client side logic by parsing the response from server
          //
          Map<?, ?> m = (Map<?, ?>)JSON.parse(resp.getEntity().toString());
          RemoteException re = JsonUtil.toRemoteException(m);
          Exception unwrapped = ((RemoteException)re).unwrapRemoteException(
              StandbyException.class);
          assertTrue (unwrapped instanceof StandbyException);
          return null;
        }
      }
    });
  }

  @SuppressWarnings("unchecked")
  private Token<DelegationTokenIdentifier> getDelegationToken(FileSystem fs,
      String renewer) throws IOException {
    final Token<?> tokens[] = fs.addDelegationTokens(renewer, null);
    assertEquals(1, tokens.length);
    return (Token<DelegationTokenIdentifier>) tokens[0];
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

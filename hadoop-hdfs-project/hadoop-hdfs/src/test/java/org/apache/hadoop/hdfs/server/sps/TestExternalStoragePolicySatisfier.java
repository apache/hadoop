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
package org.apache.hadoop.hdfs.server.sps;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SPS_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SPS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SPS_MAX_OUTSTANDING_PATHS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.sps.BlockMovementListener;
import org.apache.hadoop.hdfs.server.namenode.sps.Context;
import org.apache.hadoop.hdfs.server.namenode.sps.FileIdCollector;
import org.apache.hadoop.hdfs.server.namenode.sps.SPSService;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier;
import org.apache.hadoop.hdfs.server.namenode.sps.TestStoragePolicySatisfier;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests the external sps service plugins.
 */
public class TestExternalStoragePolicySatisfier
    extends TestStoragePolicySatisfier {
  private StorageType[][] allDiskTypes =
      new StorageType[][]{{StorageType.DISK, StorageType.DISK},
          {StorageType.DISK, StorageType.DISK},
          {StorageType.DISK, StorageType.DISK}};
  private NameNodeConnector nnc;
  private File keytabFile;
  private String principal;
  private MiniKdc kdc;
  private File baseDir;

  @After
  public void destroy() throws Exception {
    if (kdc != null) {
      kdc.stop();
      FileUtil.fullyDelete(baseDir);
    }
  }

  @Override
  public void setUp() {
    super.setUp();

    getConf().set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.EXTERNAL.toString());
  }

  @Override
  public void createCluster() throws IOException {
    getConf().setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    setCluster(startCluster(getConf(), allDiskTypes, NUM_OF_DATANODES,
        STORAGES_PER_DATANODE, CAPACITY));
    getFS();
    writeContent(FILE);
  }

  @Override
  public MiniDFSCluster startCluster(final Configuration conf,
      StorageType[][] storageTypes, int numberOfDatanodes, int storagesPerDn,
      long nodeCapacity) throws IOException {
    long[][] capacities = new long[numberOfDatanodes][storagesPerDn];
    for (int i = 0; i < numberOfDatanodes; i++) {
      for (int j = 0; j < storagesPerDn; j++) {
        capacities[i][j] = nodeCapacity;
      }
    }
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numberOfDatanodes).storagesPerDatanode(storagesPerDn)
        .storageTypes(storageTypes).storageCapacities(capacities).build();
    cluster.waitActive();

    nnc = getNameNodeConnector(getConf());

    BlockManager blkMgr = cluster.getNameNode().getNamesystem()
        .getBlockManager();
    SPSService spsService = blkMgr.getSPSManager().getInternalSPSService();
    spsService.stopGracefully();

    ExternalSPSContext context = new ExternalSPSContext(spsService,
        getNameNodeConnector(conf));

    ExternalBlockMovementListener blkMoveListener =
        new ExternalBlockMovementListener();
    ExternalSPSBlockMoveTaskHandler externalHandler =
        new ExternalSPSBlockMoveTaskHandler(conf, nnc,
            blkMgr.getSPSManager().getInternalSPSService());
    externalHandler.init();
    spsService.init(context,
        new ExternalSPSFileIDCollector(context,
            blkMgr.getSPSManager().getInternalSPSService()),
        externalHandler, blkMoveListener);
    spsService.start(true, StoragePolicySatisfierMode.EXTERNAL);
    return cluster;
  }

  public void restartNamenode() throws IOException{
    BlockManager blkMgr = getCluster().getNameNode().getNamesystem()
        .getBlockManager();
    SPSService spsService = blkMgr.getSPSManager().getInternalSPSService();
    spsService.stopGracefully();

    getCluster().restartNameNodes();
    getCluster().waitActive();
    blkMgr = getCluster().getNameNode().getNamesystem()
        .getBlockManager();
    spsService = blkMgr.getSPSManager().getInternalSPSService();
    spsService.stopGracefully();

    ExternalSPSContext context = new ExternalSPSContext(spsService,
        getNameNodeConnector(getConf()));
    ExternalBlockMovementListener blkMoveListener =
        new ExternalBlockMovementListener();
    ExternalSPSBlockMoveTaskHandler externalHandler =
        new ExternalSPSBlockMoveTaskHandler(getConf(), nnc,
            blkMgr.getSPSManager().getInternalSPSService());
    externalHandler.init();
    spsService.init(context,
        new ExternalSPSFileIDCollector(context,
            blkMgr.getSPSManager().getInternalSPSService()),
        externalHandler, blkMoveListener);
    spsService.start(true, StoragePolicySatisfierMode.EXTERNAL);
  }

  @Override
  public FileIdCollector createFileIdCollector(StoragePolicySatisfier sps,
      Context ctxt) {
    return new ExternalSPSFileIDCollector(ctxt, sps);
  }

  private class ExternalBlockMovementListener implements BlockMovementListener {

    private List<Block> actualBlockMovements = new ArrayList<>();

    @Override
    public void notifyMovementTriedBlocks(Block[] moveAttemptFinishedBlks) {
      for (Block block : moveAttemptFinishedBlks) {
        actualBlockMovements.add(block);
      }
      LOG.info("Movement attempted blocks", actualBlockMovements);
    }
  }

  private NameNodeConnector getNameNodeConnector(Configuration conf)
      throws IOException {
    final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    Assert.assertEquals(1, namenodes.size());
    final Path externalSPSPathId = new Path("/system/tmp.id");
    NameNodeConnector.checkOtherInstanceRunning(false);
    while (true) {
      try {
        final List<NameNodeConnector> nncs = NameNodeConnector
            .newNameNodeConnectors(namenodes,
                StoragePolicySatisfier.class.getSimpleName(),
                externalSPSPathId, conf,
                NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS);
        return nncs.get(0);
      } catch (IOException e) {
        LOG.warn("Failed to connect with namenode", e);
        // Ignore
      }

    }
  }

  private void initSecureConf(Configuration conf) throws Exception {
    String username = "externalSPS";
    baseDir = GenericTestUtils
        .getTestDir(TestExternalStoragePolicySatisfier.class.getSimpleName());
    FileUtil.fullyDelete(baseDir);
    Assert.assertTrue(baseDir.mkdirs());

    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, baseDir);
    kdc.start();

    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);
    KerberosName.resetDefaultRealm();
    Assert.assertTrue("Expected configuration to enable security",
        UserGroupInformation.isSecurityEnabled());

    keytabFile = new File(baseDir, username + ".keytab");
    String keytab = keytabFile.getAbsolutePath();
    // Windows will not reverse name lookup "127.0.0.1" to "localhost".
    String krbInstance = Path.WINDOWS ? "127.0.0.1" : "localhost";
    principal = username + "/" + krbInstance + "@" + kdc.getRealm();
    String spnegoPrincipal = "HTTP/" + krbInstance + "@" + kdc.getRealm();
    kdc.createPrincipal(keytabFile, username, username + "/" + krbInstance,
        "HTTP/" + krbInstance);

    conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, principal);
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, keytab);
    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, principal);
    conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, keytab);
    conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);

    conf.set(DFS_SPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_SPS_KEYTAB_FILE_KEY, keytab);
    conf.set(DFS_SPS_KERBEROS_PRINCIPAL_KEY, principal);

    String keystoresDir = baseDir.getAbsolutePath();
    String sslConfDir = KeyStoreTestUtil
        .getClasspathDir(TestExternalStoragePolicySatisfier.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);

    conf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    conf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());

    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
  }

  /**
   * Test SPS runs fine when logging in with a keytab in kerberized env. Reusing
   * testWhenStoragePolicySetToALLSSD here for basic functionality testing.
   */
  @Test(timeout = 300000)
  public void testWithKeytabs() throws Exception {
    try {
      initSecureConf(getConf());
      final UserGroupInformation ugi = UserGroupInformation
          .loginUserFromKeytabAndReturnUGI(principal,
              keytabFile.getAbsolutePath());
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          // verify that sps runs Ok.
          testWhenStoragePolicySetToALLSSD();
          // verify that UGI was logged in using keytab.
          Assert.assertTrue(UserGroupInformation.isLoginKeytabBased());
          return null;
        }
      });
    } finally {
      // Reset UGI so that other tests are not affected.
      UserGroupInformation.reset();
      UserGroupInformation.setConfiguration(new Configuration());
    }
  }

  /**
   * Test verifies that SPS call will throw exception if the call Q exceeds
   * OutstandingQueueLimit value.
   *
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testOutstandingQueueLimitExceeds() throws Exception {
    try {
      getConf().setInt(DFS_SPS_MAX_OUTSTANDING_PATHS_KEY, 3);
      createCluster();
      List<String> files = new ArrayList<>();
      files.add(FILE);
      DistributedFileSystem fs = getFS();
      BlockManager blkMgr = getCluster().getNameNode().getNamesystem()
          .getBlockManager();
      SPSService spsService = blkMgr.getSPSManager().getInternalSPSService();
      spsService.stopGracefully(); // stops SPS

      // Creates 4 more files. Send all of them for satisfying the storage
      // policy together.
      for (int i = 0; i < 3; i++) {
        String file1 = "/testOutstandingQueueLimitExceeds_" + i;
        files.add(file1);
        writeContent(file1);
        fs.satisfyStoragePolicy(new Path(file1));
      }
      String fileExceeds = "/testOutstandingQueueLimitExceeds_" + 4;
      files.add(fileExceeds);
      writeContent(fileExceeds);
      try {
        fs.satisfyStoragePolicy(new Path(fileExceeds));
        Assert.fail("Should throw exception as it exceeds "
            + "outstanding SPS call Q limit");
      } catch (IOException ioe) {
        GenericTestUtils.assertExceptionContains(
            "Outstanding satisfier queue limit: 3 exceeded, try later!", ioe);
      }
    } finally {
      shutdownCluster();
    }
  }

  /**
   * Test verifies status check when Satisfier is not running inside namenode.
   */
  @Test(timeout = 90000)
  public void testStoragePolicySatisfyPathStatus() throws Exception {
    createCluster();
    DistributedFileSystem fs = getFS();
    try {
      fs.getClient().checkStoragePolicySatisfyPathStatus(FILE);
      Assert.fail("Should throw exception as SPS is not running inside NN!");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Satisfier is not running"
          + " inside namenode, so status can't be returned.", e);
    }
  }

  /**
   * This test need not run as external scan is not a batch based scanning right
   * now.
   */
  @Ignore("ExternalFileIdCollector is not batch based right now."
      + " So, ignoring it.")
  public void testBatchProcessingForSPSDirectory() throws Exception {
  }

  /**
   * This test case is more specific to internal.
   */
  @Ignore("This test is specific to internal, so skipping here.")
  public void testWhenMoverIsAlreadyRunningBeforeStoragePolicySatisfier()
      throws Exception {
  }

  /**
   * Status won't be supported for external SPS, now. So, ignoring it.
   */
  @Ignore("Status is not supported for external SPS. So, ignoring it.")
  public void testMaxRetryForFailedBlock() throws Exception {
  }
}

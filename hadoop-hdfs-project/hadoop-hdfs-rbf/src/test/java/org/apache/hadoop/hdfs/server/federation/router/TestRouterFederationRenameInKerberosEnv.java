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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ImpersonationProvider;
import org.apache.hadoop.tools.fedbalance.DistCpProcedure;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_IMPERSONATION_PROVIDER_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.IGNORE_SECURE_PORTS_FOR_TESTING_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createFile;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.verifyFileExists;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_BANDWIDTH;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FEDERATION_RENAME_MAP;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.ZK_DTSM_ZK_AUTH_TYPE;
import static org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.ZK_DTSM_ZK_CONNECTION_STRING;
import static org.apache.hadoop.test.GenericTestUtils.getMethodName;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.SCHEDULER_JOURNAL_URI;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_PRINCIPAL;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Basic tests of router federation rename. Rename across namespaces.
 */
public class TestRouterFederationRenameInKerberosEnv
    extends ClientBaseWithFixes {

  private static final int NUM_SUBCLUSTERS = 2;
  private static final int NUM_DNS = 6;

  private static String clientPrincipal = "client@EXAMPLE.COM";
  private static String serverPrincipal =  System.getenv().get("USERNAME")
      + "/localhost@EXAMPLE.COM";
  private static String keytab = new File(
      System.getProperty("test.dir", "target"),
      UUID.randomUUID().toString())
      .getAbsolutePath();

  private static Configuration baseConf = new Configuration(false);

  private static MiniKdc kdc;

  /** Federated HDFS cluster. */
  private MiniRouterDFSCluster cluster;

  /** Random Router for this federated cluster. */
  private RouterContext router;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    // init KDC
    File workDir = new File(System.getProperty("test.dir", "target"));
    kdc = new MiniKdc(MiniKdc.createConf(), workDir);
    kdc.start();
    kdc.createPrincipal(new File(keytab), clientPrincipal, serverPrincipal);


    baseConf.setBoolean(DFSConfigKeys.HADOOP_CALLER_CONTEXT_ENABLED_KEY,
        true);

    SecurityUtil.setAuthenticationMethod(KERBEROS, baseConf);
    baseConf.set(RBFConfigKeys.DFS_ROUTER_KERBEROS_PRINCIPAL_KEY,
        serverPrincipal);
    baseConf.set(RBFConfigKeys.DFS_ROUTER_KEYTAB_FILE_KEY, keytab);
    baseConf.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
        serverPrincipal);
    baseConf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, keytab);
    baseConf.set(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY,
        serverPrincipal);
    baseConf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, keytab);
    baseConf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY,
        true);

    baseConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY,
        DFS_DATA_TRANSFER_PROTECTION_DEFAULT);
    baseConf.setBoolean(IGNORE_SECURE_PORTS_FOR_TESTING_KEY, true);
    baseConf.setClass(HADOOP_SECURITY_IMPERSONATION_PROVIDER_CLASS,
        AllowUserImpersonationProvider.class, ImpersonationProvider.class);

    DistCpProcedure.enableForTest();
  }

  /**
   * {@link ImpersonationProvider} that confirms the user doing the
   * impersonating is the same as the user running the MiniCluster.
   */
  private static class AllowUserImpersonationProvider extends Configured
      implements ImpersonationProvider {
    public void init(String configurationPrefix) {
      // Do nothing
    }

    public void authorize(UserGroupInformation user, InetAddress remoteAddress)
        throws AuthorizationException {
      try {
        if (!user.getRealUser().getShortUserName()
            .equals(UserGroupInformation.getCurrentUser().getShortUserName())) {
          throw new AuthorizationException();
        }
      } catch (IOException ioe) {
        throw new AuthorizationException(ioe);
      }
    }
  }

  @AfterClass
  public static void globalTearDown() {
    kdc.stop();
    DistCpProcedure.disableForTest();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    cluster.shutdown();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    cluster = new MiniRouterDFSCluster(false, NUM_SUBCLUSTERS);
    cluster.setNumDatanodesPerNameservice(NUM_DNS);
    cluster.addNamenodeOverrides(baseConf);
    cluster.setIndependentDNs();
    // Start NNs and DNs and wait until ready.
    cluster.startCluster();

    // Start routers, enable router federation rename.
    String journal = "hdfs://" + cluster.getCluster().getNameNode(1)
        .getClientNamenodeAddress() + "/journal";
    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .routerRenameOption()
        .set(SCHEDULER_JOURNAL_URI, journal)
        .set(DFS_ROUTER_FEDERATION_RENAME_MAP, "1")
        .set(DFS_ROUTER_FEDERATION_RENAME_BANDWIDTH, "1")
        .set(ZK_DTSM_ZK_CONNECTION_STRING, hostPort)
        .set(ZK_DTSM_ZK_AUTH_TYPE, "none")
        .set(RM_PRINCIPAL, serverPrincipal)
        .build();
    // We decrease the DN cache times to make the test faster.
    routerConf.setTimeDuration(
        RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 1, TimeUnit.SECONDS);
    cluster.addRouterOverrides(baseConf);
    cluster.addRouterOverrides(routerConf);
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();

    // We decrease the DN heartbeat expire interval to make them dead faster
    cluster.getCluster().getNamesystem(0).getBlockManager()
        .getDatanodeManager().setHeartbeatInterval(1);
    cluster.getCluster().getNamesystem(1).getBlockManager()
        .getDatanodeManager().setHeartbeatInterval(1);
    cluster.getCluster().getNamesystem(0).getBlockManager()
        .getDatanodeManager().setHeartbeatExpireInterval(3000);
    cluster.getCluster().getNamesystem(1).getBlockManager()
        .getDatanodeManager().setHeartbeatExpireInterval(3000);

    // Create mock locations
    cluster.installMockLocations();

    // Create test fixtures on NN
    cluster.createTestDirectoriesNamenode();

    // Random router for this test
    RouterContext rndRouter = cluster.getRandomRouter();
    setRouter(rndRouter);
  }

  protected void prepareEnv(FileSystem fs, Path path, Path renamedPath)
      throws IOException {
    // Set permission of parent to 777.
    fs.setPermission(path.getParent(),
        FsPermission.createImmutable((short)511));
    fs.setPermission(renamedPath.getParent(),
        FsPermission.createImmutable((short)511));
    // Create src path and file.
    fs.mkdirs(path);
    String file = path.toString() + "/file";
    createFile(fs, file, 32);
    verifyFileExists(fs, path.toString());
    verifyFileExists(fs, file);
  }

  protected void testRenameDir(RouterContext testRouter, String path,
      String renamedPath, boolean exceptionExpected, Callable<Object> call)
      throws IOException {
    prepareEnv(testRouter.getFileSystem(), new Path(path),
        new Path(renamedPath));
    // rename
    boolean exceptionThrown = false;
    try {
      call.call();
      assertFalse(verifyFileExists(testRouter.getFileSystem(), path));
      assertTrue(verifyFileExists(testRouter.getFileSystem(),
          renamedPath + "/file"));
    } catch (Exception ex) {
      exceptionThrown = true;
      assertTrue(verifyFileExists(testRouter.getFileSystem(),
          path + "/file"));
      assertFalse(verifyFileExists(testRouter.getFileSystem(), renamedPath));
    } finally {
      FileContext fileContext = testRouter.getFileContext();
      fileContext.delete(new Path(path), true);
      fileContext.delete(new Path(renamedPath), true);
    }
    if (exceptionExpected) {
      // Error was expected.
      assertTrue(exceptionThrown);
    } else {
      // No error was expected.
      assertFalse(exceptionThrown);
    }
  }

  protected void setRouter(RouterContext r) throws IOException {
    this.router = r;
  }

  @Test
  public void testClientRename() throws IOException {
    String ns0 = cluster.getNameservices().get(0);
    String ns1 = cluster.getNameservices().get(1);
    // Test successfully rename a dir to a destination that is in a different
    // namespace.
    String dir =
        cluster.getFederatedTestDirectoryForNS(ns0) + "/" + getMethodName();
    String renamedDir =
        cluster.getFederatedTestDirectoryForNS(ns1) + "/" + getMethodName();
    testRenameDir(router, dir, renamedDir, false, () -> {
      UserGroupInformation ugi = UserGroupInformation
          .loginUserFromKeytabAndReturnUGI(clientPrincipal, keytab);
      ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> {
        DFSClient client = router.getClient();
        ClientProtocol clientProtocol = client.getNamenode();
        clientProtocol.rename(dir, renamedDir);
        return null;
      });
      return null;
    });

  }

}
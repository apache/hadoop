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

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.simulateSlowNamenode;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.apache.hadoop.test.GenericTestUtils.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMetrics;
import org.apache.hadoop.hdfs.server.federation.metrics.NamenodeBeanMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.NamenodeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RemoteException;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.function.Supplier;

/**
 * Test retry behavior of the Router RPC Client.
 */
public class TestRouterRPCClientRetries {

  private static StateStoreDFSCluster cluster;
  private static NamenodeContext nnContext1;
  private static RouterContext routerContext;
  private static MembershipNamenodeResolver resolver;
  private static ClientProtocol routerProtocol;

  @Rule
  public final Timeout testTimeout = new Timeout(100000);

  @Before
  public void setUp() throws Exception {
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .metrics()
        .admin()
        .rpc()
        .build();
    routerConf.setTimeDuration(
        RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 1, TimeUnit.SECONDS);

    // reduce IPC client connection retry times and interval time
    Configuration clientConf = new Configuration(false);
    clientConf.setInt(
        CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    clientConf.setInt(
        CommonConfigurationKeys.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY, 100);

    // Set the DNs to belong to only one subcluster
    cluster.setIndependentDNs();

    cluster.addRouterOverrides(routerConf);
    // override some settings for the client
    cluster.startCluster(clientConf);
    cluster.startRouters();
    cluster.waitClusterUp();

    nnContext1 = cluster.getNamenode(cluster.getNameservices().get(0), null);
    routerContext = cluster.getRandomRouter();
    resolver = (MembershipNamenodeResolver) routerContext.getRouter()
        .getNamenodeResolver();
    routerProtocol = routerContext.getClient().getNamenode();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.stopRouter(routerContext);
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testRetryWhenAllNameServiceDown() throws Exception {
    // shutdown the dfs cluster
    MiniDFSCluster dfsCluster = cluster.getCluster();
    dfsCluster.shutdown();

    // register an invalid namenode report
    registerInvalidNameReport();

    // Create a directory via the router
    String dirPath = "/testRetryWhenClusterisDown";
    FsPermission permission = new FsPermission("705");
    try {
      routerProtocol.mkdirs(dirPath, permission, false);
      fail("Should have thrown RemoteException error.");
    } catch (RemoteException e) {
      String ns0 = cluster.getNameservices().get(0);
      assertExceptionContains(
          "No namenodes available under nameservice " + ns0, e);
    }

    // Verify the retry times, it should only retry one time.
    FederationRPCMetrics rpcMetrics = routerContext.getRouter()
        .getRpcServer().getRPCMetrics();
    assertEquals(1, rpcMetrics.getProxyOpRetries());
  }

  @Test
  public void testRetryWhenOneNameServiceDown() throws Exception {
    // shutdown the dfs cluster
    MiniDFSCluster dfsCluster = cluster.getCluster();
    dfsCluster.shutdownNameNode(0);

    // register an invalid namenode report
    registerInvalidNameReport();

    DFSClient client = nnContext1.getClient();
    // Renew lease for the DFS client, it will succeed.
    routerProtocol.renewLease(client.getClientName());

    // Verify the retry times, it will retry one time for ns0.
    FederationRPCMetrics rpcMetrics = routerContext.getRouter()
        .getRpcServer().getRPCMetrics();
    assertEquals(1, rpcMetrics.getProxyOpRetries());
  }

  /**
   * Register an invalid namenode report.
   * @throws IOException
   */
  private void registerInvalidNameReport() throws IOException {
    String ns0 = cluster.getNameservices().get(0);
    List<? extends FederationNamenodeContext> origin = resolver
        .getNamenodesForNameserviceId(ns0);
    FederationNamenodeContext nnInfo = origin.get(0);
    NamenodeStatusReport report = new NamenodeStatusReport(ns0,
        nnInfo.getNamenodeId(), nnInfo.getRpcAddress(),
        nnInfo.getServiceAddress(), nnInfo.getLifelineAddress(),
        nnInfo.getWebScheme(), nnInfo.getWebAddress());
    report.setRegistrationValid(false);
    assertTrue(resolver.registerNamenode(report));
    resolver.loadCache(true);
  }

  @Test
  public void testNamenodeMetricsSlow() throws Exception {
    final Router router = routerContext.getRouter();
    final NamenodeBeanMetrics metrics = router.getNamenodeMetrics();

    // Initially, there are 4 DNs in total
    final String jsonString0 = metrics.getLiveNodes();
    assertEquals(4, getNumDatanodes(jsonString0));

    // The response should be cached
    assertEquals(jsonString0, metrics.getLiveNodes());

    // Check that the cached value gets updated eventually
    waitUpdateLiveNodes(jsonString0, metrics);
    final String jsonString2 = metrics.getLiveNodes();
    assertNotEquals(jsonString0, jsonString2);
    assertEquals(4, getNumDatanodes(jsonString2));

    // Making subcluster0 slow to reply, should only get DNs from nn1
    MiniDFSCluster dfsCluster = cluster.getCluster();
    NameNode nn0 = dfsCluster.getNameNode(0);
    simulateSlowNamenode(nn0, 3);
    waitUpdateLiveNodes(jsonString2, metrics);
    final String jsonString3 = metrics.getLiveNodes();
    assertEquals(2, getNumDatanodes(jsonString3));

    // Making subcluster1 slow to reply, shouldn't get any DNs
    NameNode nn1 = dfsCluster.getNameNode(1);
    simulateSlowNamenode(nn1, 3);
    waitUpdateLiveNodes(jsonString3, metrics);
    final String jsonString4 = metrics.getLiveNodes();
    assertEquals(0, getNumDatanodes(jsonString4));
  }

  /**
   * Get the number of nodes in a JSON string.
   * @param jsonString JSON string containing nodes.
   * @return Number of nodes.
   * @throws JSONException If the JSON string is not properly formed.
   */
  private static int getNumDatanodes(final String jsonString)
      throws JSONException {
    JSONObject jsonObject = new JSONObject(jsonString);
    if (jsonObject.length() == 0) {
      return 0;
    }
    return jsonObject.names().length();
  }

  /**
   * Wait until the cached live nodes value is updated.
   * @param oldValue Old cached value.
   * @param metrics Namenode metrics beans to get the live nodes from.
   * @throws Exception If it cannot wait.
   */
  private static void waitUpdateLiveNodes(
      final String oldValue, final NamenodeBeanMetrics metrics)
          throws Exception {
    waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return !oldValue.equals(metrics.getLiveNodes());
      }
    }, 500, 5 * 1000);
  }
}

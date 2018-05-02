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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMetrics;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test the Router overload control which rejects requests when the RPC client
 * is overloaded. This feature is managed by
 * {@link RBFConfigKeys#DFS_ROUTER_CLIENT_REJECT_OVERLOAD}.
 */
public class TestRouterClientRejectOverload {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterClientRejectOverload.class);

  private StateStoreDFSCluster cluster;

  @After
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void setupCluster(boolean overloadControl) throws Exception {
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .metrics()
        .admin()
        .rpc()
        .build();

    // Reduce the number of RPC clients threads to overload the Router easy
    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_CLIENT_THREADS_SIZE, 4);
    // Overload control
    routerConf.setBoolean(
        RBFConfigKeys.DFS_ROUTER_CLIENT_REJECT_OVERLOAD, overloadControl);

    // No need for datanodes as we use renewLease() for testing
    cluster.setNumDatanodesPerNameservice(0);

    cluster.addRouterOverrides(routerConf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();
  }

  @Test
  public void testWithoutOverloadControl() throws Exception {
    setupCluster(false);

    // Nobody should get overloaded
    testOverloaded(0);

    // Set subcluster 0 as slow
    MiniDFSCluster dfsCluster = cluster.getCluster();
    NameNode nn0 = dfsCluster.getNameNode(0);
    simulateSlowNamenode(nn0, 1);

    // Nobody should get overloaded, but it will be really slow
    testOverloaded(0);

    // No rejected requests expected
    for (RouterContext router : cluster.getRouters()) {
      FederationRPCMetrics rpcMetrics =
          router.getRouter().getRpcServer().getRPCMetrics();
      assertEquals(0, rpcMetrics.getProxyOpFailureClientOverloaded());
    }
  }

  @Test
  public void testOverloadControl() throws Exception {
    setupCluster(true);

    List<RouterContext> routers = cluster.getRouters();
    FederationRPCMetrics rpcMetrics0 =
        routers.get(0).getRouter().getRpcServer().getRPCMetrics();
    FederationRPCMetrics rpcMetrics1 =
        routers.get(1).getRouter().getRpcServer().getRPCMetrics();

    // Nobody should get overloaded
    testOverloaded(0);
    assertEquals(0, rpcMetrics0.getProxyOpFailureClientOverloaded());
    assertEquals(0, rpcMetrics1.getProxyOpFailureClientOverloaded());

    // Set subcluster 0 as slow
    MiniDFSCluster dfsCluster = cluster.getCluster();
    NameNode nn0 = dfsCluster.getNameNode(0);
    simulateSlowNamenode(nn0, 1);

    // The subcluster should be overloaded now and reject 4-5 requests
    testOverloaded(4, 6);
    assertTrue(rpcMetrics0.getProxyOpFailureClientOverloaded()
        + rpcMetrics1.getProxyOpFailureClientOverloaded() >= 4);

    // Client using HA with 2 Routers
    // A single Router gets overloaded, but 2 will handle it
    Configuration clientConf = cluster.getRouterClientConf();

    // Each Router should get a similar number of ops (>=8) out of 2*10
    long iniProxyOps0 = rpcMetrics0.getProxyOps();
    long iniProxyOps1 = rpcMetrics1.getProxyOps();
    testOverloaded(0, 0, new URI("hdfs://fed/"), clientConf, 10);
    long proxyOps0 = rpcMetrics0.getProxyOps() - iniProxyOps0;
    long proxyOps1 = rpcMetrics1.getProxyOps() - iniProxyOps1;
    assertEquals(2 * 10, proxyOps0 + proxyOps1);
    assertTrue(proxyOps0 + " operations: not distributed", proxyOps0 >= 8);
    assertTrue(proxyOps1 + " operations: not distributed", proxyOps1 >= 8);
  }

  private void testOverloaded(int expOverload) throws Exception {
    testOverloaded(expOverload, expOverload);
  }

  private void testOverloaded(int expOverloadMin, int expOverloadMax)
      throws Exception {
    RouterContext routerContext = cluster.getRandomRouter();
    URI address = routerContext.getFileSystemURI();
    Configuration conf = new HdfsConfiguration();
    testOverloaded(expOverloadMin, expOverloadMax, address, conf, 10);
  }

  /**
   * Test if the Router gets overloaded by submitting requests in parallel.
   * We check how many requests got rejected at the end.
   * @param expOverloadMin Min number of requests expected as overloaded.
   * @param expOverloadMax Max number of requests expected as overloaded.
   * @param address Destination address.
   * @param conf Configuration of the client.
   * @param numOps Number of operations to submit.
   * @throws Exception If it cannot perform the test.
   */
  private void testOverloaded(int expOverloadMin, int expOverloadMax,
      final URI address, final Configuration conf, final int numOps)
          throws Exception {

    // Submit renewLease() ops which go to all subclusters
    final AtomicInteger overloadException = new AtomicInteger();
    ExecutorService exec = Executors.newFixedThreadPool(numOps);
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < numOps; i++) {
      // Stagger the operations a little (50ms)
      final int sleepTime = i * 50;
      Future<?> future = exec.submit(new Runnable() {
        @Override
        public void run() {
          DFSClient routerClient = null;
          try {
            Thread.sleep(sleepTime);
            routerClient = new DFSClient(address, conf);
            String clientName = routerClient.getClientName();
            ClientProtocol routerProto = routerClient.getNamenode();
            routerProto.renewLease(clientName);
          } catch (RemoteException re) {
            IOException ioe = re.unwrapRemoteException();
            assertTrue("Wrong exception: " + ioe,
                ioe instanceof StandbyException);
            assertExceptionContains("is overloaded", ioe);
            overloadException.incrementAndGet();
          } catch (IOException e) {
            fail("Unexpected exception: " + e);
          } catch (InterruptedException e) {
            fail("Cannot sleep: " + e);
          } finally {
            if (routerClient != null) {
              try {
                routerClient.close();
              } catch (IOException e) {
                LOG.error("Cannot close the client");
              }
            }
          }
        }
      });
      futures.add(future);
    }
    // Wait until all the requests are done
    while (!futures.isEmpty()) {
      futures.remove(0).get();
    }
    exec.shutdown();

    int num = overloadException.get();
    if (expOverloadMin == expOverloadMax) {
      assertEquals(expOverloadMin, num);
    } else {
      assertTrue("Expected >=" + expOverloadMin + " but was " + num,
          num >= expOverloadMin);
      assertTrue("Expected <=" + expOverloadMax + " but was " + num,
          num <= expOverloadMax);
    }
  }
}

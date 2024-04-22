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
package org.apache.hadoop.hdfs.server.federation.fairness;

import static org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessConstants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.RemoteMethod;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcClient;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the Router handlers fairness control rejects and accepts requests.
 */
@RunWith(Parameterized.class)
public class TestRouterHandlersFairness {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterHandlersFairness.class);

  private StateStoreDFSCluster cluster;
  private Map<String, Integer> expectedHandlerPerNs;
  private Class<RouterRpcFairnessPolicyController> policyControllerClass;
  private int handlerCount;
  private Map<String, String> configuration;

  /**
   * Initialize test parameters.
   *
   * @param policyControllerClass RouterRpcFairnessPolicyController type.
   * @param handlerCount The total number of handlers in the router.
   * @param configuration Custom configuration.
   * @param expectedHandlerPerNs The number of handlers expected for each ns.
   */
  public TestRouterHandlersFairness(
      Class<RouterRpcFairnessPolicyController> policyControllerClass, int handlerCount,
      Map<String, String> configuration, Map<String, Integer> expectedHandlerPerNs) {
    this.expectedHandlerPerNs = expectedHandlerPerNs;
    this.policyControllerClass = policyControllerClass;
    this.handlerCount = handlerCount;
    this.configuration = configuration;
  }

  @Parameterized.Parameters
  public static Collection primes() {
    return Arrays.asList(new Object[][]{
        {
            //  Test StaticRouterRpcFairnessPolicyController.
            StaticRouterRpcFairnessPolicyController.class,
            3,
            setConfiguration(null),
            expectedHandlerPerNs("ns0:1, ns1:1, concurrent:1")
        },
        {
            // Test ProportionRouterRpcFairnessPolicyController.
            ProportionRouterRpcFairnessPolicyController.class,
            20,
            setConfiguration(
                "dfs.federation.router.fairness.handler.proportion.ns0=0.5, " +
                "dfs.federation.router.fairness.handler.proportion.ns1=0.8, " +
                "dfs.federation.router.fairness.handler.proportion.concurrent=1"
            ),
            expectedHandlerPerNs("ns0:10, ns1:16, concurrent:20")
        }
    });
  }

  @After
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void setupCluster(boolean fairnessEnable, boolean ha)
      throws Exception {
    LOG.info("Test {}", policyControllerClass.getSimpleName());
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(ha, 2);
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .rpc()
        .build();

    // Fairness control
    if (fairnessEnable) {
      routerConf.setClass(
          RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
          this.policyControllerClass,
          RouterRpcFairnessPolicyController.class);
    }

    routerConf.setTimeDuration(DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT, 10, TimeUnit.MILLISECONDS);

    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY, this.handlerCount);

    for(Map.Entry<String, String> conf : configuration.entrySet()) {
      routerConf.set(conf.getKey(), conf.getValue());
    }

    // Datanodes not needed for this test.
    cluster.setNumDatanodesPerNameservice(0);

    cluster.addRouterOverrides(routerConf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();
  }

  @Test
  public void testFairnessControlOff() throws Exception {
    setupCluster(false, false);
    startLoadTest(false);
  }

  @Test
  public void testFairnessControlOn() throws Exception {
    setupCluster(true, false);
    startLoadTest(true);
  }

  /**
   * Ensure that the semaphore is not acquired,
   * when invokeSequential or invokeConcurrent throws any exception.
   */
  @Test
  public void testReleasedWhenExceptionOccurs() throws Exception{
    setupCluster(true, false);
    RouterContext routerContext = cluster.getRandomRouter();
    RouterRpcClient rpcClient =
        routerContext.getRouter().getRpcServer().getRPCClient();
    // Mock an ActiveNamenodeResolver and inject it into RouterRpcClient,
    // so RouterRpcClient's getOrderedNamenodes method will report an exception.
    ActiveNamenodeResolver mockNamenodeResolver = mock(ActiveNamenodeResolver.class);
    Field field = rpcClient.getClass().getDeclaredField("namenodeResolver");
    field.setAccessible(true);
    field.set(rpcClient, mockNamenodeResolver);

    // Use getFileInfo test invokeSequential.
    DFSClient client = routerContext.getClient();
    int availablePermits =
        rpcClient.getRouterRpcFairnessPolicyController().getAvailablePermits("ns0");
    LambdaTestUtils.intercept(IOException.class,  () -> {
      LOG.info("Use getFileInfo test invokeSequential.");
      client.getFileInfo("/test.txt");
    });
    // Ensure that the semaphore is not acquired.
    assertEquals(availablePermits,
        rpcClient.getRouterRpcFairnessPolicyController().getAvailablePermits("ns0"));

    // Use renewLease test invokeConcurrent.
    Collection<RemoteLocation> locations = new ArrayList<>();
    locations.add(new RemoteLocation("ns0", "/", "/"));
    RemoteMethod renewLease = new RemoteMethod(
        "renewLease",
        new Class[]{java.lang.String.class, java.util.List.class},
        new Object[]{null, null});
    availablePermits =
        rpcClient.getRouterRpcFairnessPolicyController().getAvailablePermits("ns0");
    LambdaTestUtils.intercept(IOException.class,  () -> {
      LOG.info("Use renewLease test invokeConcurrent.");
      rpcClient.invokeConcurrent(locations, renewLease);
    });
    // Ensure that the semaphore is not acquired.
    assertEquals(availablePermits,
        rpcClient.getRouterRpcFairnessPolicyController().getAvailablePermits("ns0"));
  }

  /**
   * Start a generic load test as a client against a cluster which has either
   * fairness configured or not configured. Test will spawn a set of 100
   * threads to simulate concurrent request to test routers. If fairness is
   * enabled, the test will successfully report the failure of some threads
   * to continue with StandbyException. If fairness is not configured, all
   * threads of the same test should successfully complete all the rpcs.
   *
   * @param fairness Flag to indicate if fairness management is on/off.
   * @throws Exception Throws exception.
   */
  private void startLoadTest(boolean fairness)
      throws Exception {

    // Concurrent requests
    startLoadTest(true, fairness);

    // Sequential requests
    startLoadTest(false, fairness);
  }

  private void startLoadTest(final boolean isConcurrent, final boolean fairness)
      throws Exception {

    RouterContext routerContext = cluster.getRandomRouter();
    URI address = routerContext.getFileSystemURI();
    Configuration conf = new HdfsConfiguration();
    final int numOps = 10;
    AtomicInteger overloadException = new AtomicInteger();

    // Test when handlers are overloaded
    if (fairness) {
      if (isConcurrent) {
        LOG.info("Taking fanout lock first");
        // take the lock for concurrent NS to block fanout calls
        for(int i = 0; i < expectedHandlerPerNs.get(CONCURRENT_NS); i++) {
          assertTrue(routerContext.getRouter().getRpcServer()
              .getRPCClient().getRouterRpcFairnessPolicyController()
              .acquirePermit(CONCURRENT_NS));
        }
      } else {
        for (String ns : cluster.getNameservices()) {
          LOG.info("Taking lock first for ns: {}", ns);
          for(int i = 0; i < expectedHandlerPerNs.get(ns); i++) {
            assertTrue(routerContext.getRouter().getRpcServer()
                .getRPCClient().getRouterRpcFairnessPolicyController()
                .acquirePermit(ns));
          }
        }
      }
    }
    int originalRejectedPermits = getTotalRejectedPermits(routerContext);

    // |- All calls should fail since permits not released
    innerCalls(address, numOps, isConcurrent, conf, overloadException);

    int latestRejectedPermits = getTotalRejectedPermits(routerContext);
    assertEquals(latestRejectedPermits - originalRejectedPermits,
        overloadException.get());

    if (fairness) {
      assertTrue(overloadException.get() > 0);
      if (isConcurrent) {
        LOG.info("Release fanout lock that was taken before test");
        // take the lock for concurrent NS to block fanout calls
        for(int i = 0; i < expectedHandlerPerNs.get(CONCURRENT_NS); i++) {
          routerContext.getRouter().getRpcServer()
              .getRPCClient().getRouterRpcFairnessPolicyController()
              .releasePermit(CONCURRENT_NS);
        }
      } else {
        for (String ns : cluster.getNameservices()) {
          for(int i = 0; i < expectedHandlerPerNs.get(ns); i++) {
            routerContext.getRouter().getRpcServer()
                .getRPCClient().getRouterRpcFairnessPolicyController()
                .releasePermit(ns);
          }
        }
      }
    } else {
      assertEquals("Number of failed RPCs without fairness configured",
          0, overloadException.get());
    }

    // Test when handlers are not overloaded
    int originalAcceptedPermits = getTotalAcceptedPermits(routerContext);
    overloadException = new AtomicInteger();

    // |- All calls should succeed since permits not acquired
    innerCalls(address, numOps, isConcurrent, conf, overloadException);

    int latestAcceptedPermits = getTotalAcceptedPermits(routerContext);
    assertEquals(latestAcceptedPermits - originalAcceptedPermits, numOps);
    assertEquals(overloadException.get(), 0);
  }

  private void invokeSequential(ClientProtocol routerProto) throws IOException {
    routerProto.getFileInfo("/test.txt");
  }

  private void invokeConcurrent(ClientProtocol routerProto, String clientName)
      throws IOException {
    routerProto.renewLease(clientName, null);
  }

  private int getTotalRejectedPermits(RouterContext routerContext) {
    int totalRejectedPermits = 0;
    for (String ns : cluster.getNameservices()) {
      totalRejectedPermits += routerContext.getRouterRpcClient()
          .getRejectedPermitForNs(ns);
    }
    totalRejectedPermits += routerContext.getRouterRpcClient()
        .getRejectedPermitForNs(CONCURRENT_NS);
    return totalRejectedPermits;
  }

  private int getTotalAcceptedPermits(RouterContext routerContext) {
    int totalAcceptedPermits = 0;
    for (String ns : cluster.getNameservices()) {
      totalAcceptedPermits += routerContext.getRouterRpcClient()
          .getAcceptedPermitForNs(ns);
    }
    totalAcceptedPermits += routerContext.getRouterRpcClient()
        .getAcceptedPermitForNs(CONCURRENT_NS);
    return totalAcceptedPermits;
  }

  private void innerCalls(URI address, int numOps, boolean isConcurrent,
      Configuration conf, AtomicInteger overloadException) throws IOException {
    for (int i = 0; i < numOps; i++) {
      DFSClient routerClient = null;
      try {
        routerClient = new DFSClient(address, conf);
        String clientName = routerClient.getClientName();
        ClientProtocol routerProto = routerClient.getNamenode();
        if (isConcurrent) {
          invokeConcurrent(routerProto, clientName);
        } else {
          invokeSequential(routerProto);
        }
      } catch (RemoteException re) {
        IOException ioe = re.unwrapRemoteException();
        assertTrue("Wrong exception: " + ioe,
            ioe instanceof StandbyException);
        assertExceptionContains("is overloaded for NS", ioe);
        overloadException.incrementAndGet();
      } catch (Throwable e) {
        throw e;
      } finally {
        if (routerClient != null) {
          try {
            routerClient.close();
          } catch (IOException e) {
            LOG.error("Cannot close the client");
          }
        }
      }
      overloadException.get();
    }
  }

  private static Map<String, Integer> expectedHandlerPerNs(String str) {
    Map<String, Integer> handlersPerNsMap = new HashMap<>();
    if (str == null) {
      return handlersPerNsMap;
    }
    String[] tmpStrs = str.split(", ");
    for(String tmpStr : tmpStrs) {
      String[] handlersPerNs = tmpStr.split(":");
      if (handlersPerNs.length != 2) {
        continue;
      }
      handlersPerNsMap.put(handlersPerNs[0], Integer.valueOf(handlersPerNs[1]));
    }
    return handlersPerNsMap;
  }

  private static Map<String, String> setConfiguration(String str) {
    Map<String, String> conf = new HashMap<>();
    if (str == null) {
      return conf;
    }
    String[] tmpStrs = str.split(", ");
    for(String tmpStr : tmpStrs) {
      String[] configKV = tmpStr.split("=");
      if (configKV.length != 2) {
        continue;
      }
      conf.put(configKV[0], configKV[1]);
    }
    return conf;
  }
}

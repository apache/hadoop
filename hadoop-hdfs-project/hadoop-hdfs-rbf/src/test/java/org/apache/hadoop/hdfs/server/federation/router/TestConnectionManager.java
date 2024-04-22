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
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RouterFederatedStateProto;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;

/**
 * Test functionalities of {@link ConnectionManager}, which manages a pool
 * of connections to NameNodes.
 */
public class TestConnectionManager {
  private Configuration conf;
  private ConnectionManager connManager;
  private static final String[] TEST_GROUP = new String[]{"TEST_GROUP"};
  private static final UserGroupInformation TEST_USER1 =
      UserGroupInformation.createUserForTesting("user1", TEST_GROUP);
  private static final UserGroupInformation TEST_USER2 =
      UserGroupInformation.createUserForTesting("user2", TEST_GROUP);
  private static final UserGroupInformation TEST_USER3 =
      UserGroupInformation.createUserForTesting("user3", TEST_GROUP);
  private static final String TEST_NN_ADDRESS = "nn1:8080";
  private static final String UNRESOLVED_TEST_NN_ADDRESS = "unknownhost:8080";

  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    connManager = new ConnectionManager(conf);
    NetUtils.addStaticResolution("nn1", "localhost");
    NetUtils.createSocketAddrForHost("nn1", 8080);
    connManager.start();
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @After
  public void shutdown() {
    if (connManager != null) {
      connManager.close();
    }
  }

  @Test
  public void testCleanup() throws Exception {
    Map<ConnectionPoolId, ConnectionPool> poolMap = connManager.getPools();

    ConnectionPool pool1 = new ConnectionPool(conf, TEST_NN_ADDRESS, TEST_USER1,
        0, 10, 0.5f, ClientProtocol.class, null);
    addConnectionsToPool(pool1, 9, 4);
    poolMap.put(
        new ConnectionPoolId(TEST_USER1, TEST_NN_ADDRESS, ClientProtocol.class),
        pool1);

    ConnectionPool pool2 = new ConnectionPool(conf, TEST_NN_ADDRESS, TEST_USER2,
        0, 10, 0.5f, ClientProtocol.class, null);
    addConnectionsToPool(pool2, 10, 10);
    poolMap.put(
        new ConnectionPoolId(TEST_USER2, TEST_NN_ADDRESS, ClientProtocol.class),
        pool2);

    checkPoolConnections(TEST_USER1, 9, 4);
    checkPoolConnections(TEST_USER2, 10, 10);

    // Clean up first pool, one connection should be removed, and second pool
    // should remain the same.
    connManager.cleanup(pool1);
    checkPoolConnections(TEST_USER1, 8, 4);
    checkPoolConnections(TEST_USER2, 10, 10);

    // Clean up the first pool again, it should have no effect since it reached
    // the MIN_ACTIVE_RATIO.
    connManager.cleanup(pool1);
    checkPoolConnections(TEST_USER1, 8, 4);
    checkPoolConnections(TEST_USER2, 10, 10);

    // Make sure the number of connections doesn't go below minSize
    ConnectionPool pool3 = new ConnectionPool(conf, TEST_NN_ADDRESS, TEST_USER3,
        2, 10, 0.5f, ClientProtocol.class, null);
    addConnectionsToPool(pool3, 8, 0);
    poolMap.put(
        new ConnectionPoolId(TEST_USER3, TEST_NN_ADDRESS, ClientProtocol.class),
        pool3);
    checkPoolConnections(TEST_USER3, 10, 0);
    for (int i = 0; i < 10; i++) {
      connManager.cleanup(pool3);
    }
    checkPoolConnections(TEST_USER3, 2, 0);
    // With active connections added to pool, make sure it honors the
    // MIN_ACTIVE_RATIO again
    addConnectionsToPool(pool3, 8, 2);
    checkPoolConnections(TEST_USER3, 10, 2);
    for (int i = 0; i < 10; i++) {
      connManager.cleanup(pool3);
    }
    checkPoolConnections(TEST_USER3, 4, 2);
  }

  @Test
  public void testGetConnectionWithConcurrency() throws Exception {
    Map<ConnectionPoolId, ConnectionPool> poolMap = connManager.getPools();
    Configuration copyConf = new Configuration(conf);
    copyConf.setInt(RBFConfigKeys.DFS_ROUTER_MAX_CONCURRENCY_PER_CONNECTION_KEY, 20);

    ConnectionPool pool = new ConnectionPool(
        copyConf, TEST_NN_ADDRESS, TEST_USER1, 1, 10, 0.5f,
        ClientProtocol.class, null);
    poolMap.put(
        new ConnectionPoolId(TEST_USER1, TEST_NN_ADDRESS, ClientProtocol.class),
        pool);
    assertEquals(1, pool.getNumConnections());
    // one connection can process the maximum number of requests concurrently.
    for (int i = 0; i < 20; i++) {
      ConnectionContext cc = pool.getConnection();
      assertTrue(cc.isUsable());
      cc.getClient();
    }
    assertEquals(1, pool.getNumConnections());

    // Ask for more and this returns an unusable connection
    ConnectionContext cc1 = pool.getConnection();
    assertTrue(cc1.isActive());
    assertFalse(cc1.isUsable());

    // add a new connection into pool
    pool.addConnection(pool.newConnection());
    // will return the new connection
    ConnectionContext cc2 = pool.getConnection();
    assertTrue(cc2.isUsable());
    cc2.getClient();

    assertEquals(2, pool.getNumConnections());

    checkPoolConnections(TEST_USER1, 2, 2);
  }

  @Test
  public void testConnectionCreatorWithException() throws Exception {
    // Create a bad connection pool pointing to unresolvable namenode address.
    ConnectionPool badPool = new ConnectionPool(
        conf, UNRESOLVED_TEST_NN_ADDRESS, TEST_USER1, 0, 10, 0.5f,
        ClientProtocol.class, null);
    BlockingQueue<ConnectionPool> queue = new ArrayBlockingQueue<>(1);
    queue.add(badPool);
    ConnectionManager.ConnectionCreator connectionCreator =
        new ConnectionManager.ConnectionCreator(queue);
    connectionCreator.setDaemon(true);
    connectionCreator.start();
    // Wait to make sure async thread is scheduled and picks
    GenericTestUtils.waitFor(queue::isEmpty, 50, 5000);
    // At this point connection creation task should be definitely picked up.
    assertTrue(queue.isEmpty());
    // At this point connection thread should still be alive.
    assertTrue(connectionCreator.isAlive());
    // Stop the thread as test is successful at this point
    connectionCreator.interrupt();
  }

  @Test
  public void testGetConnectionWithException() throws Exception {
    String exceptionCause = "java.net.UnknownHostException: unknownhost";
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage(exceptionCause);

    // Create a bad connection pool pointing to unresolvable namenode address.
    ConnectionPool badPool = new ConnectionPool(
        conf, UNRESOLVED_TEST_NN_ADDRESS, TEST_USER1, 1, 10, 0.5f,
        ClientProtocol.class, null);
  }

  @Test
  public void testGetConnection() throws Exception {
    Map<ConnectionPoolId, ConnectionPool> poolMap = connManager.getPools();
    final int totalConns = 10;
    int activeConns = 5;

    ConnectionPool pool = new ConnectionPool(conf, TEST_NN_ADDRESS, TEST_USER1,
        0, 10, 0.5f, ClientProtocol.class, null);
    addConnectionsToPool(pool, totalConns, activeConns);
    poolMap.put(
        new ConnectionPoolId(TEST_USER1, TEST_NN_ADDRESS, ClientProtocol.class),
        pool);

    // All remaining connections should be usable
    final int remainingSlots = totalConns - activeConns;
    for (int i = 0; i < remainingSlots; i++) {
      ConnectionContext cc = pool.getConnection();
      assertTrue(cc.isUsable());
      cc.getClient();
      activeConns++;
    }

    checkPoolConnections(TEST_USER1, totalConns, activeConns);

    // Ask for more and this returns an active connection
    ConnectionContext cc = pool.getConnection();
    assertTrue(cc.isActive());
  }

  @Test
  public void testValidClientIndex() throws Exception {
    ConnectionPool pool = new ConnectionPool(conf, TEST_NN_ADDRESS, TEST_USER1,
        2, 2, 0.5f, ClientProtocol.class, null);
    for(int i = -3; i <= 3; i++) {
      pool.getClientIndex().set(i);
      ConnectionContext conn = pool.getConnection();
      assertNotNull(conn);
      assertTrue(conn.isUsable());
    }
  }

  @Test
  public void getGetConnectionNamenodeProtocol() throws Exception {
    Map<ConnectionPoolId, ConnectionPool> poolMap = connManager.getPools();
    final int totalConns = 10;
    int activeConns = 5;

    ConnectionPool pool = new ConnectionPool(conf, TEST_NN_ADDRESS, TEST_USER1,
        0, 10, 0.5f, NamenodeProtocol.class, null);
    addConnectionsToPool(pool, totalConns, activeConns);
    poolMap.put(
        new ConnectionPoolId(
            TEST_USER1, TEST_NN_ADDRESS, NamenodeProtocol.class),
        pool);

    // All remaining connections should be usable
    final int remainingSlots = totalConns - activeConns;
    for (int i = 0; i < remainingSlots; i++) {
      ConnectionContext cc = pool.getConnection();
      assertTrue(cc.isUsable());
      cc.getClient();
      activeConns++;
    }

    checkPoolConnections(TEST_USER1, totalConns, activeConns);

    // Ask for more and this returns an active connection
    ConnectionContext cc = pool.getConnection();
    assertTrue(cc.isActive());
  }

  private void addConnectionsToPool(ConnectionPool pool, int numTotalConn,
      int numActiveConn) throws IOException {
    for (int i = 0; i < numTotalConn; i++) {
      ConnectionContext cc = pool.newConnection();
      pool.addConnection(cc);
      if (i < numActiveConn) {
        cc.getClient();
      }
    }
  }

  private void checkPoolConnections(UserGroupInformation ugi,
      int numOfConns, int numOfActiveConns) {
    boolean connPoolFoundForUser = false;
    for (Map.Entry<ConnectionPoolId, ConnectionPool> e :
        connManager.getPools().entrySet()) {
      if (e.getKey().getUgi() == ugi) {
        assertEquals(numOfConns, e.getValue().getNumConnections());
        assertEquals(numOfActiveConns, e.getValue().getNumActiveConnections());
        // idle + active = total connections
        assertEquals(numOfConns - numOfActiveConns,
            e.getValue().getNumIdleConnections());
        connPoolFoundForUser = true;
      }
    }
    if (!connPoolFoundForUser) {
      fail("Connection pool not found for user " + ugi.getUserName());
    }
  }

  @Test
  public void testAdvanceClientStateId() throws IOException {
    // Start one ConnectionManager
    Configuration tmpConf = new Configuration();
    ConnectionManager tmpConnManager = new ConnectionManager(tmpConf);
    tmpConnManager.start();
    Map<ConnectionPoolId, ConnectionPool> poolMap = tmpConnManager.getPools();

    // Mock one Server.Call with FederatedNamespaceState that ns0 = 1L.
    Server.Call mockCall1 = new Server.Call(1, 1, null, null,
        RPC.RpcKind.RPC_BUILTIN, new byte[] {1, 2, 3});
    Map<String, Long> nsStateId = new HashMap<>();
    nsStateId.put("ns0", 1L);
    RouterFederatedStateProto.Builder stateBuilder = RouterFederatedStateProto.newBuilder();
    nsStateId.forEach(stateBuilder::putNamespaceStateIds);
    mockCall1.setFederatedNamespaceState(stateBuilder.build().toByteString());

    Server.getCurCall().set(mockCall1);

    // Create one new connection pool
    tmpConnManager.getConnection(TEST_USER1, TEST_NN_ADDRESS, NamenodeProtocol.class, "ns0");
    assertEquals(1, poolMap.size());
    ConnectionPoolId connectionPoolId = new ConnectionPoolId(TEST_USER1,
        TEST_NN_ADDRESS, NamenodeProtocol.class);
    ConnectionPool pool = poolMap.get(connectionPoolId);
    assertEquals(1L, pool.getPoolAlignmentContext().getPoolLocalStateId());

    // Mock one Server.Call with FederatedNamespaceState that ns0 = 2L.
    Server.Call mockCall2 = new Server.Call(2, 1, null, null,
        RPC.RpcKind.RPC_BUILTIN, new byte[] {1, 2, 3});
    nsStateId.clear();
    nsStateId.put("ns0", 2L);
    stateBuilder = RouterFederatedStateProto.newBuilder();
    nsStateId.forEach(stateBuilder::putNamespaceStateIds);
    mockCall2.setFederatedNamespaceState(stateBuilder.build().toByteString());

    Server.getCurCall().set(mockCall2);

    // Get one existed connection for ns0
    tmpConnManager.getConnection(TEST_USER1, TEST_NN_ADDRESS, NamenodeProtocol.class, "ns0");
    assertEquals(1, poolMap.size());
    pool = poolMap.get(connectionPoolId);
    assertEquals(2L, pool.getPoolAlignmentContext().getPoolLocalStateId());
  }

  @Test
  public void testConfigureConnectionActiveRatio() throws IOException {
    // test 1 conn below the threshold and these conns are closed
    testConnectionCleanup(0.8f, 10, 7, 9);

    // test 2 conn below the threshold and these conns are closed
    testConnectionCleanup(0.8f, 10, 6, 8);
  }

  @Test
  public void testConnectionCreatorWithSamePool() throws IOException {
    Configuration tmpConf = new Configuration();
    // Set DFS_ROUTER_MAX_CONCURRENCY_PER_CONNECTION_KEY to 0
    // for ensuring a pool will be offered in the creatorQueue
    tmpConf.setInt(
        RBFConfigKeys.DFS_ROUTER_MAX_CONCURRENCY_PER_CONNECTION_KEY, 0);
    ConnectionManager tmpConnManager = new ConnectionManager(tmpConf);
    tmpConnManager.start();
    // Close ConnectionCreator thread to make sure that new connection will not initialize.
    tmpConnManager.closeConnectionCreator();
    // Create same connection pool for simulating concurrency scenario
    for (int i = 0; i < 3; i++) {
      tmpConnManager.getConnection(TEST_USER1, TEST_NN_ADDRESS,
          NamenodeProtocol.class, "ns0");
    }
    assertEquals(1, tmpConnManager.getNumCreatingConnections());

    tmpConnManager.getConnection(TEST_USER2, TEST_NN_ADDRESS,
        NamenodeProtocol.class, "ns0");
    assertEquals(2, tmpConnManager.getNumCreatingConnections());
  }

  private void testConnectionCleanup(float ratio, int totalConns,
      int activeConns, int leftConns) throws IOException {
    Configuration tmpConf = new Configuration();
    // Set dfs.federation.router.connection.min-active-ratio
    tmpConf.setFloat(
        RBFConfigKeys.DFS_ROUTER_NAMENODE_CONNECTION_MIN_ACTIVE_RATIO, ratio);
    ConnectionManager tmpConnManager = new ConnectionManager(tmpConf);
    tmpConnManager.start();

    // Create one new connection pool
    tmpConnManager.getConnection(TEST_USER1, TEST_NN_ADDRESS,
        NamenodeProtocol.class, "ns0");

    Map<ConnectionPoolId, ConnectionPool> poolMap = tmpConnManager.getPools();
    ConnectionPoolId connectionPoolId = new ConnectionPoolId(TEST_USER1,
        TEST_NN_ADDRESS, NamenodeProtocol.class);
    ConnectionPool pool = poolMap.get(connectionPoolId);

    // Test min active ratio is as set value
    assertEquals(ratio, pool.getMinActiveRatio(), 0.001f);

    pool.getConnection().getClient();
    // Test there is one active connection in pool
    assertEquals(1, pool.getNumActiveConnections());

    // Add other active-1 connections / totalConns-1 connections to pool
    addConnectionsToPool(pool, totalConns - 1, activeConns - 1);

    // There are activeConn connections.
    // We can cleanup the pool
    tmpConnManager.cleanup(pool);
    assertEquals(leftConns, pool.getNumConnections());

    tmpConnManager.close();
  }

  @Test
  public void testUnsupportedProtoExceptionMsg() throws Exception {
    LambdaTestUtils.intercept(IllegalStateException.class,
        "Unsupported protocol for connection to NameNode: "
            + TestConnectionManager.class.getName(),
        () -> ConnectionPool.newConnection(conf, TEST_NN_ADDRESS, TEST_USER1,
            TestConnectionManager.class, false, 0, null));
  }
}

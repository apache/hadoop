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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    connManager = new ConnectionManager(conf);
    NetUtils.addStaticResolution("nn1", "localhost");
    NetUtils.createSocketAddrForHost("nn1", 8080);
    connManager.start();
  }

  @After
  public void shutdown() {
    if (connManager != null) {
      connManager.close();
    }
  }

  @Test
  public void testCleanup() throws Exception {
    Map<ConnectionPoolId, ConnectionPool> poolMap = connManager.getPools();

    ConnectionPool pool1 = new ConnectionPool(
        conf, TEST_NN_ADDRESS, TEST_USER1, 0, 10);
    addConnectionsToPool(pool1, 9, 4);
    poolMap.put(new ConnectionPoolId(TEST_USER1, TEST_NN_ADDRESS), pool1);

    ConnectionPool pool2 = new ConnectionPool(
        conf, TEST_NN_ADDRESS, TEST_USER2, 0, 10);
    addConnectionsToPool(pool2, 10, 10);
    poolMap.put(new ConnectionPoolId(TEST_USER2, TEST_NN_ADDRESS), pool2);

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
    ConnectionPool pool3 = new ConnectionPool(
        conf, TEST_NN_ADDRESS, TEST_USER3, 2, 10);
    addConnectionsToPool(pool3, 10, 0);
    poolMap.put(new ConnectionPoolId(TEST_USER2, TEST_NN_ADDRESS), pool3);
    connManager.cleanup(pool3);
    checkPoolConnections(TEST_USER3, 2, 0);
    // With active connections added to pool, make sure it honors the
    // MIN_ACTIVE_RATIO again
    addConnectionsToPool(pool3, 10, 2);
    connManager.cleanup(pool3);
    checkPoolConnections(TEST_USER3, 4, 2);
  }

  @Test
  public void testGetConnection() throws Exception {
    Map<ConnectionPoolId, ConnectionPool> poolMap = connManager.getPools();
    final int totalConns = 10;
    int activeConns = 5;

    ConnectionPool pool = new ConnectionPool(
        conf, TEST_NN_ADDRESS, TEST_USER1, 0, 10);
    addConnectionsToPool(pool, totalConns, activeConns);
    poolMap.put(new ConnectionPoolId(TEST_USER1, TEST_NN_ADDRESS), pool);

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
    for (Map.Entry<ConnectionPoolId, ConnectionPool> e :
        connManager.getPools().entrySet()) {
      if (e.getKey().getUgi() == ugi) {
        assertEquals(numOfConns, e.getValue().getNumConnections());
        assertEquals(numOfActiveConns, e.getValue().getNumActiveConnections());
      }
    }
  }

}

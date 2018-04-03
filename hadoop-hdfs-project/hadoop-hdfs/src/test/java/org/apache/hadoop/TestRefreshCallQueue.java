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

package org.apache.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.BindException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.ipc.FairCallQueue;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.After;
import org.junit.Test;

public class TestRefreshCallQueue {
  private MiniDFSCluster cluster;
  private Configuration config;
  static int mockQueueConstructions;
  static int mockQueuePuts;
  private int nnPort = 0;

  private void setUp(Class<?> queueClass) throws IOException {
    int portRetries = 5;
    Random rand = new Random();
    for (; portRetries > 0; --portRetries) {
      // Pick a random port in the range [30000,60000).
      nnPort = 30000 + rand.nextInt(30000);
      config = new Configuration();
      String callQueueConfigKey = "ipc." + nnPort + ".callqueue.impl";
      config.setClass(callQueueConfigKey, queueClass, BlockingQueue.class);
      config.set("hadoop.security.authorization", "true");

      FileSystem.setDefaultUri(config, "hdfs://localhost:" + nnPort);
      try {
        cluster = new MiniDFSCluster.Builder(config).nameNodePort(nnPort)
            .build();
        cluster.waitActive();
        break;
      } catch (BindException be) {
        // Retry with a different port number.
      }
    }
    if (portRetries == 0) {
      // Bail if we get very unlucky with our choice of ports.
      fail("Failed to pick an ephemeral port for the NameNode RPC server.");
    }
  }

  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @SuppressWarnings("serial")
  public static class MockCallQueue<E> extends LinkedBlockingQueue<E> {
    public MockCallQueue(int levels, int cap, String ns, Configuration conf) {
      super(cap);
      mockQueueConstructions++;
    }

    public void put(E e) throws InterruptedException {
      super.put(e);
      mockQueuePuts++;
    }
  }

  // Returns true if mock queue was used for put
  public boolean canPutInMockQueue() throws IOException {
    FileSystem fs = FileSystem.get(config);
    int putsBefore = mockQueuePuts;
    fs.exists(new Path("/")); // Make an RPC call
    fs.close();
    return mockQueuePuts > putsBefore;
  }

  @Test
  public void testRefresh() throws Exception {
    // We want to count additional events, so we reset here
    mockQueueConstructions = 0;
    mockQueuePuts = 0;
    setUp(MockCallQueue.class);

    assertTrue("Mock queue should have been constructed",
        mockQueueConstructions > 0);
    assertTrue("Puts are routed through MockQueue", canPutInMockQueue());
    int lastMockQueueConstructions = mockQueueConstructions;

    // Replace queue with the queue specified in core-site.xml, which would be
    // the LinkedBlockingQueue
    DFSAdmin admin = new DFSAdmin(config);
    String [] args = new String[]{"-refreshCallQueue"};
    int exitCode = admin.run(args);
    assertEquals("DFSAdmin should return 0", 0, exitCode);

    assertEquals("Mock queue should have no additional constructions",
        lastMockQueueConstructions, mockQueueConstructions);
    try {
      assertFalse("Puts are routed through LBQ instead of MockQueue",
          canPutInMockQueue());
    } catch (IOException ioe) {
      fail("Could not put into queue at all");
    }
  }

  @Test
  public void testRefreshCallQueueWithFairCallQueue() throws Exception {
    setUp(FairCallQueue.class);
    boolean oldValue = DefaultMetricsSystem.inMiniClusterMode();

    // throw an error when we double-initialize JvmMetrics
    DefaultMetricsSystem.setMiniClusterMode(false);
    int serviceHandlerCount = config.getInt(
        DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY,
        DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT);
    NameNodeRpcServer rpcServer = (NameNodeRpcServer) cluster.getNameNodeRpc();
    // check callqueue size
    assertEquals(CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT
        * serviceHandlerCount, rpcServer.getClientRpcServer().getMaxQueueSize());
    // Replace queue and update queue size
    config.setInt(CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
        150);
    try {
      rpcServer.getClientRpcServer().refreshCallQueue(config);
    } catch (Exception e) {
      Throwable cause = e.getCause();
      if ((cause instanceof MetricsException)
          && cause.getMessage().contains(
              "Metrics source DecayRpcSchedulerMetrics2.ipc." + nnPort
                  + " already exists!")) {
        fail("DecayRpcScheduler metrics should be unregistered before"
            + " reregister");
      }
      throw e;
    } finally {
      DefaultMetricsSystem.setMiniClusterMode(oldValue);
    }
    // check callQueueSize has changed
    assertEquals(150 * serviceHandlerCount, rpcServer.getClientRpcServer()
        .getMaxQueueSize());
 }

}